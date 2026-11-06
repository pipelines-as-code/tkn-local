package localexecution

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"reflect"
	"strings"
	"time"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	"github.com/pipelines-as-code/tkn-local/internal/iostreams"
	tektonconfig "github.com/tektoncd/pipeline/pkg/apis/config"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

const (
	preparationTimeout = 5 * time.Minute
	cleanupTimeout     = 2 * time.Minute
)

type RunOptions struct {
	Namespace       string
	Context         string
	Kubeconfig      string
	TektonNamespace string
	StorageSize     resource.Quantity
	StorageClass    string
	Timeout         time.Duration
	KeepResources   bool
}

type Execution struct {
	clients      *cluster.Clients
	config       *rest.Config
	streams      *iostreams.IOStreams
	options      RunOptions
	source       *Snapshot
	prepared     *Prepared
	contract     *executionContract
	security     securityDefaults
	pipelineRun  *tektonv1.PipelineRun
	pvc          *corev1.PersistentVolumeClaim
	pod          *corev1.Pod
	tkn          string
	uploadSource func(context.Context, *corev1.Pod, io.Reader) error
	followLogs   func(context.Context, string) error
}

func CheckTkn(ctx context.Context) (string, error) {
	binary, err := exec.LookPath("tkn")
	if err != nil {
		return "", fmt.Errorf("local execution requires tkn for live logs: %w", err)
	}
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(checkCtx, binary, "pipelinerun", "logs", "--help").Output()
	if err != nil {
		return "", fmt.Errorf("check tkn log support: %w", err)
	}
	for _, flag := range []string{"--follow", "--kubeconfig", "--context", "--namespace"} {
		if !strings.Contains(string(output), flag) {
			return "", fmt.Errorf("installed tkn lacks required log flag %s", flag)
		}
	}
	return binary, nil
}

func Prepare(ctx context.Context, clients *cluster.Clients, streams *iostreams.IOStreams, source *Snapshot, prepared *Prepared, options RunOptions, binary string) (*Execution, error) {
	ctx, cancel := context.WithTimeout(ctx, preparationTimeout)
	defer cancel()
	config := rest.CopyConfig(clients.Config)
	if options.Namespace == "" || options.Context == "" || options.Timeout <= 0 || options.StorageSize.Sign() <= 0 {
		return nil, fmt.Errorf("namespace, resolved context, positive timeout and storage size are required")
	}
	if err := checkAccess(ctx, clients, options.Namespace); err != nil {
		return nil, err
	}
	if err := checkStorage(ctx, clients, streams, &options); err != nil {
		return nil, err
	}
	security, err := loadSecurityDefaults(ctx, clients, options.Namespace)
	if err != nil {
		return nil, err
	}
	if err := checkCoscheduling(ctx, clients, options.TektonNamespace, security.OpenShift); err != nil {
		return nil, err
	}
	id := "tkn-local-" + strings.ToLower(rand.Text()[:16])
	pr := prepared.PipelineRun.DeepCopy()
	if err := applySecurityDefaults(pr, prepared.Workspace, security); err != nil {
		return nil, err
	}
	pr.Name = id
	pr.GenerateName = ""
	pr.Namespace = options.Namespace
	pr.APIVersion = "tekton.dev/v1"
	pr.Kind = "PipelineRun"
	pr.Spec.Status = tektonv1.PipelineRunSpecStatusPending
	if pr.Labels == nil {
		pr.Labels = map[string]string{}
	}
	if pr.Annotations == nil {
		pr.Annotations = map[string]string{}
	}
	pr.Labels[RunIDLabel] = id
	pr.Annotations["local.tekton.dev/source-sha256"] = source.Digest
	pr.Annotations["local.tekton.dev/source-head"] = source.Head
	pr.Annotations["local.tekton.dev/source-dirty"] = fmt.Sprint(source.Dirty)
	pr.Annotations["local.tekton.dev/definition-sha256"] = prepared.Digest
	if pr.Spec.Timeouts == nil {
		pr.Spec.Timeouts = &tektonv1.TimeoutFields{}
	}
	if pr.Spec.Timeouts.Pipeline == nil || pr.Spec.Timeouts.Pipeline.Duration == 0 || pr.Spec.Timeouts.Pipeline.Duration > options.Timeout {
		pr.Spec.Timeouts.Pipeline = &metav1.Duration{Duration: options.Timeout}
	}

	binding := tektonv1.WorkspaceBinding{Name: prepared.Workspace, SubPath: "worktree", PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: id + "-source"}}
	found := false
	for i, w := range pr.Spec.Workspaces {
		if w.Name == prepared.Workspace {
			pr.Spec.Workspaces[i] = binding
			found = true
		}
	}
	if !found {
		pr.Spec.Workspaces = append(pr.Spec.Workspaces, binding)
	}
	admitted, err := clients.Tekton.TektonV1().PipelineRuns(options.Namespace).Create(ctx, pr, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		return nil, fmt.Errorf("dry-run pending PipelineRun with a future PVC: %w", err)
	}
	if admitted.Spec.Status != tektonv1.PipelineRunSpecStatusPending {
		return nil, fmt.Errorf("admission did not preserve PipelineRunPending")
	}
	inspected := admitted.DeepCopy()
	for i, binding := range inspected.Spec.Workspaces {
		if binding.Name == prepared.Workspace {
			inspected.Spec.Workspaces[i] = tektonv1.WorkspaceBinding{Name: prepared.Workspace, EmptyDir: &corev1.EmptyDirVolumeSource{}}
		}
	}
	if _, err := validatePipeline(inspected, prepared.Workspace); err != nil {
		return nil, fmt.Errorf("admitted PipelineRun: %w", err)
	}
	contract, err := admittedContract(admitted, prepared.Workspace)
	if err != nil {
		return nil, err
	}
	contract.OpenShift = security.OpenShift
	for _, identity := range contract.Identities {
		if _, err := clients.Kube.CoreV1().ServiceAccounts(options.Namespace).Get(ctx, identity.ServiceAccount, metav1.GetOptions{}); err != nil {
			return nil, fmt.Errorf("task %s service account %s: %w", identity.Task, identity.ServiceAccount, err)
		}
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: id + "-source", Namespace: options.Namespace, Labels: map[string]string{RunIDLabel: id}}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: ptr.To(options.StorageClass), Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: options.StorageSize}}}}
	admittedPVC, err := clients.Kube.CoreV1().PersistentVolumeClaims(options.Namespace).Create(ctx, pvc, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		return nil, fmt.Errorf("dry-run source PVC: %w", err)
	}
	if admittedPVC.Spec.VolumeName != "" || admittedPVC.Spec.Selector != nil || admittedPVC.Spec.DataSource != nil || admittedPVC.Spec.DataSourceRef != nil || (admittedPVC.Spec.VolumeMode != nil && *admittedPVC.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) || len(admittedPVC.Spec.AccessModes) != 1 || admittedPVC.Spec.AccessModes[0] != corev1.ReadWriteOnce || admittedPVC.Spec.StorageClassName == nil || *admittedPVC.Spec.StorageClassName != options.StorageClass || admittedPVC.Spec.Resources.Requests.Storage().Cmp(options.StorageSize) != 0 {
		return nil, fmt.Errorf("admission changed the fresh RWO filesystem PVC contract")
	}
	pod := uploaderPod(id+"-upload", options.Namespace, pvc.Name, contract)
	uploaderAccount, err := clients.Kube.CoreV1().ServiceAccounts(options.Namespace).Get(ctx, pod.Spec.ServiceAccountName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("uploader service account: %w", err)
	}
	pod.Spec.ImagePullSecrets = uploaderAccount.ImagePullSecrets
	pod.Labels = map[string]string{RunIDLabel: id}
	admittedPod, err := clients.Kube.CoreV1().Pods(options.Namespace).Create(ctx, pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		return nil, fmt.Errorf("dry-run restricted uploader: %w", err)
	}
	if err := validateUploader(admittedPod, pod, security); err != nil {
		return nil, err
	}
	execution := &Execution{clients: clients, config: config, streams: streams, options: options, source: source, prepared: prepared, contract: contract, security: security, pipelineRun: admitted, pvc: admittedPVC, pod: admittedPod, tkn: binary}
	execution.uploadSource = execution.upload
	execution.followLogs = execution.logs
	return execution, nil
}

func checkAccess(ctx context.Context, clients *cluster.Clients, namespace string) error {
	resources := []struct {
		group, resource string
		verbs           []string
	}{
		{"tekton.dev", "pipelineruns", []string{"create", "get", "list", "watch", "update", "delete"}},
		{"tekton.dev", "pipelineruns/finalizers", []string{"update"}},
		{"tekton.dev", "taskruns", []string{"get", "list", "watch"}},
		{"", "pods", []string{"create", "get", "list", "watch", "delete"}},
		{"", "persistentvolumeclaims", []string{"create", "get", "delete"}},
		{"", "pods/exec", []string{"get", "create"}},
		{"", "pods/log", []string{"get"}},
		{"", "serviceaccounts", []string{"get"}},
	}
	for _, res := range resources {
		resource, subresource, _ := strings.Cut(res.resource, "/")
		for _, verb := range res.verbs {
			review, err := clients.Kube.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: namespace, Verb: verb, Group: res.group, Resource: resource, Subresource: subresource}}}, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("check permission %s %s: %w", verb, res.resource, err)
			}
			if !review.Status.Allowed {
				return fmt.Errorf("namespace %s requires %s on %s: %s", namespace, verb, res.resource, review.Status.Reason)
			}
		}
	}
	return nil
}

func checkStorage(ctx context.Context, clients *cluster.Clients, streams *iostreams.IOStreams, options *RunOptions) error {
	if options.StorageClass == "" {
		classes, err := clients.Kube.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("discover default StorageClass (or specify --storage-class): %w", err)
		}
		for _, class := range classes.Items {
			if class.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" || class.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
				if options.StorageClass != "" {
					return fmt.Errorf("multiple default StorageClasses; specify --storage-class")
				}
				options.StorageClass = class.Name
			}
		}
	}
	if options.StorageClass == "" {
		return fmt.Errorf("no default StorageClass; specify --storage-class")
	}
	class, err := clients.Kube.StorageV1().StorageClasses().Get(ctx, options.StorageClass, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("inspect StorageClass %s: %w", options.StorageClass, err)
	}
	if class.ReclaimPolicy != nil && *class.ReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
		fmt.Fprintln(streams.ErrOut, "Warning: StorageClass retains backend storage after PVC deletion.")
	}
	return nil
}

func checkCoscheduling(ctx context.Context, clients *cluster.Clients, namespace string, openShift bool) error {
	nodes, err := clients.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err == nil && len(nodes.Items) == 1 {
		return nil
	}
	if namespace == "" {
		namespace = "tekton-pipelines"
		if openShift {
			namespace = "openshift-pipelines"
		}
	}
	config, err := clients.Kube.CoreV1().ConfigMaps(namespace).Get(ctx, "feature-flags", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("cannot establish multi-node RWO scheduling: read feature-flags in namespace %s (override with --tekton-namespace): %w", namespace, err)
	}
	flags, err := tektonconfig.NewFeatureFlagsFromMap(config.Data)
	if err != nil {
		return err
	}
	switch flags.Coschedule {
	case tektonconfig.CoscheduleWorkspaces, tektonconfig.CoschedulePipelineRuns, tektonconfig.CoscheduleIsolatePipelineRun:
		return nil
	default:
		return fmt.Errorf("multi-node RWO source requires Tekton coschedule=workspaces or pipelineruns")
	}
}

func validateUploader(admitted, requested *corev1.Pod, security securityDefaults) error {
	expected := requested.DeepCopy()
	if admitted.Spec.SecurityContext == nil || expected.Spec.SecurityContext == nil {
		return fmt.Errorf("uploader requires a non-root Pod security context")
	}
	if err := normalizePodSecurity(admitted.Spec.SecurityContext, expected.Spec.SecurityContext, security); err != nil {
		return err
	}
	if expected.Spec.HostUsers == nil && admitted.Spec.HostUsers != nil && !*admitted.Spec.HostUsers {
		expected.Spec.HostUsers = admitted.Spec.HostUsers
	}
	if expected.Spec.DNSPolicy == "" && admitted.Spec.DNSPolicy == corev1.DNSClusterFirst {
		expected.Spec.DNSPolicy = admitted.Spec.DNSPolicy
	}
	if expected.Spec.SchedulerName == "" && admitted.Spec.SchedulerName == "default-scheduler" {
		expected.Spec.SchedulerName = admitted.Spec.SchedulerName
	}
	if expected.Spec.TerminationGracePeriodSeconds == nil && admitted.Spec.TerminationGracePeriodSeconds != nil && *admitted.Spec.TerminationGracePeriodSeconds == 30 {
		expected.Spec.TerminationGracePeriodSeconds = admitted.Spec.TerminationGracePeriodSeconds
	}
	if expected.Spec.EnableServiceLinks == nil && admitted.Spec.EnableServiceLinks != nil && *admitted.Spec.EnableServiceLinks {
		expected.Spec.EnableServiceLinks = admitted.Spec.EnableServiceLinks
	}
	if expected.Spec.Priority == nil && admitted.Spec.Priority != nil && *admitted.Spec.Priority == 0 {
		expected.Spec.Priority = admitted.Spec.Priority
	}
	if expected.Spec.PreemptionPolicy == nil && admitted.Spec.PreemptionPolicy != nil && *admitted.Spec.PreemptionPolicy == corev1.PreemptLowerPriority {
		expected.Spec.PreemptionPolicy = admitted.Spec.PreemptionPolicy
	}
	for i := range expected.Spec.Containers {
		if i >= len(admitted.Spec.Containers) {
			break
		}
		container := &expected.Spec.Containers[i]
		actual := admitted.Spec.Containers[i]
		if container.SecurityContext == nil || actual.SecurityContext == nil {
			return fmt.Errorf("uploader container security context was removed")
		}
		if err := normalizeContainerSecurity(actual.SecurityContext, container.SecurityContext, security); err != nil {
			return err
		}
		if container.ImagePullPolicy == "" && actual.ImagePullPolicy == corev1.PullIfNotPresent {
			container.ImagePullPolicy = actual.ImagePullPolicy
		}
		if container.TerminationMessagePath == "" && actual.TerminationMessagePath == "/dev/termination-log" {
			container.TerminationMessagePath = actual.TerminationMessagePath
		}
		if container.TerminationMessagePolicy == "" && actual.TerminationMessagePolicy == corev1.TerminationMessageReadFile {
			container.TerminationMessagePolicy = actual.TerminationMessagePolicy
		}
	}
	if admitted.Spec.DeprecatedServiceAccount == requested.Spec.ServiceAccountName {
		expected.Spec.DeprecatedServiceAccount = admitted.Spec.DeprecatedServiceAccount
	}
	for _, toleration := range admitted.Spec.Tolerations {
		found := false
		for _, authored := range requested.Spec.Tolerations {
			if reflect.DeepEqual(toleration, authored) {
				found = true
			}
		}
		standard := toleration.Operator == corev1.TolerationOpExists && toleration.Effect == corev1.TaintEffectNoExecute && (toleration.Key == "node.kubernetes.io/not-ready" || toleration.Key == "node.kubernetes.io/unreachable") && toleration.TolerationSeconds != nil && *toleration.TolerationSeconds == 300
		standard = standard || toleration == (corev1.Toleration{Key: "node.kubernetes.io/memory-pressure", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
		if !found && !standard {
			return fmt.Errorf("admission introduced unsupported uploader toleration: %+v", toleration)
		}
	}
	expected.Spec.Tolerations = admitted.Spec.Tolerations
	if !reflect.DeepEqual(admitted.Spec, expected.Spec) {
		return fmt.Errorf("admission changed uploader security, contents or placement; this configuration is unsupported")
	}
	return nil
}

func (e *Execution) Preview() {
	host := e.config.Host
	if parsed, err := url.Parse(host); err == nil {
		parsed.User = nil
		parsed.RawQuery = ""
		parsed.Fragment = ""
		host = parsed.String()
	}
	fmt.Fprintf(e.streams.ErrOut, "Cluster: %s\nContext: %s\nNamespace: %s\nSource: %s (%d files, %d bytes)\nWorkspace: %s\n", host, e.options.Context, e.options.Namespace, e.source.Root, len(e.source.Files), e.source.Bytes, e.prepared.Workspace)
	for _, identity := range e.contract.Identities {
		fmt.Fprintf(e.streams.ErrOut, "Task %s: service account %s\n", identity.Task, identity.ServiceAccount)
	}
	for _, warning := range e.source.Warnings {
		fmt.Fprintf(e.streams.ErrOut, "Warning: %s\n", warning)
	}
	fmt.Fprintln(e.streams.ErrOut, "Source includes tracked and unignored files, including any credentials they contain.")
	fmt.Fprintln(e.streams.ErrOut, "Tasks run repository code with the listed accounts and can access their namespace credentials and network.")
	if e.options.KeepResources {
		fmt.Fprintln(e.streams.ErrOut, "Resources and uploaded source will be retained until explicitly deleted.")
	}
}
