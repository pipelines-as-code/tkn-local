package localexecution

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	tektonpod "github.com/tektoncd/pipeline/pkg/apis/pipeline/pod"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
)

const (
	uidRangeAnnotation       = "openshift.io/sa.scc.uid-range"
	groupsAnnotation         = "openshift.io/sa.scc.supplemental-groups"
	mcsAnnotation            = "openshift.io/sa.scc.mcs"
	defaultUser        int64 = 65532
)

type securityDefaults struct {
	OpenShift    bool
	UIDMin       int64
	UIDMax       int64
	Group        int64
	SELinuxLevel string
}

func loadSecurityDefaults(ctx context.Context, clients *cluster.Clients, namespace string) (securityDefaults, error) {
	ns, err := clients.Kube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil && !apierrors.IsForbidden(err) {
		return securityDefaults{}, fmt.Errorf("read namespace security defaults: %w", err)
	}
	if ns != nil && ns.Annotations[uidRangeAnnotation] != "" {
		return namespaceSecurityDefaults(ns)
	}
	groups, discoveryErr := clients.Kube.Discovery().ServerGroups()
	if discoveryErr != nil {
		return securityDefaults{}, fmt.Errorf("discover cluster security policy: %w", discoveryErr)
	}
	for _, group := range groups.Groups {
		if group.Name == "security.openshift.io" {
			if err != nil {
				return securityDefaults{}, fmt.Errorf("OpenShift requires read access to namespace %s for its assigned IDs: %w", namespace, err)
			}
			return securityDefaults{}, fmt.Errorf("namespace %s has no assigned OpenShift UID range yet", namespace)
		}
	}
	return securityDefaults{Group: defaultUser}, nil
}

func namespaceSecurityDefaults(ns *corev1.Namespace) (securityDefaults, error) {
	minimum, maximum, err := parseIDBlock(ns.Annotations[uidRangeAnnotation])
	if err != nil {
		return securityDefaults{}, fmt.Errorf("namespace %s has invalid %s: %w", ns.Name, uidRangeAnnotation, err)
	}
	groups, ok := ns.Annotations[groupsAnnotation]
	if !ok {
		groups = ns.Annotations[uidRangeAnnotation]
	}
	var group int64
	for i, block := range strings.Split(groups, ",") {
		start, _, err := parseIDBlock(block)
		if err != nil {
			return securityDefaults{}, fmt.Errorf("namespace %s has invalid supplemental groups: %w", ns.Name, err)
		}
		if i == 0 {
			group = start
		}
	}
	// OpenShift's preallocated FSGroup is the first block's minimum, not its whole range
	return securityDefaults{OpenShift: true, UIDMin: minimum, UIDMax: maximum, Group: group, SELinuxLevel: ns.Annotations[mcsAnnotation]}, nil
}

func parseIDBlock(block string) (int64, int64, error) {
	first, last, count := strings.Cut(block, "/")
	if !count {
		first, last, _ = strings.Cut(block, "-")
	}
	start, startErr := strconv.ParseUint(first, 10, 32)
	end, endErr := strconv.ParseUint(last, 10, 32)
	const maxID = uint64(1<<32 - 2)
	if startErr != nil || endErr != nil || start == 0 || end == 0 || start > maxID {
		return 0, 0, fmt.Errorf("expected a positive start/count or start-end ID block")
	}
	if count {
		end = start + end - 1
	}
	if end < start || end > maxID {
		return 0, 0, fmt.Errorf("invalid or overflowing ID block")
	}
	return int64(start), int64(end), nil
}

func sourceConsumer(task tektonv1.PipelineTask, workspace string) bool {
	for _, binding := range task.Workspaces {
		if binding.Workspace == workspace {
			return true
		}
	}
	return false
}

func applySecurityDefaults(pr *tektonv1.PipelineRun, workspace string, defaults securityDefaults) error {
	group := defaults.Group
	var authored *int64
	for _, task := range pipelineTasks(pr) {
		if !sourceConsumer(task, workspace) {
			continue
		}
		template := pr.DeepCopy().GetTaskRunSpec(task.Name).PodTemplate
		if template == nil || template.SecurityContext == nil || template.SecurityContext.FSGroup == nil {
			continue
		}
		value := *template.SecurityContext.FSGroup
		if value <= 0 || (defaults.OpenShift && value != defaults.Group) {
			return fmt.Errorf("task %q fsGroup %d is incompatible with the namespace's shared source group %d", task.Name, value, defaults.Group)
		}
		if authored != nil && value != *authored {
			return fmt.Errorf("all source-consuming Tasks must share fsGroup")
		}
		authored, group = ptr.To(value), value
	}
	for _, task := range pipelineTasks(pr) {
		if !sourceConsumer(task, workspace) {
			continue
		}
		template := pr.DeepCopy().GetTaskRunSpec(task.Name).PodTemplate
		if template == nil {
			template = &tektonpod.PodTemplate{}
		}
		if template.SecurityContext == nil {
			template.SecurityContext = &corev1.PodSecurityContext{}
		}
		sc := template.SecurityContext
		sc.FSGroup = ptr.To(group)
		if sc.RunAsNonRoot == nil {
			sc.RunAsNonRoot = ptr.To(true)
		}
		if !defaults.OpenShift && sc.RunAsUser == nil {
			sc.RunAsUser = ptr.To(defaultUser)
		}
		if err := defaults.validateUID(sc.RunAsUser); err != nil {
			return fmt.Errorf("task %q: %w", task.Name, err)
		}
		label := sc.SELinuxOptions.DeepCopy()
		if label != nil && label.Level == "" {
			label.Level = defaults.SELinuxLevel
		}
		if err := defaults.validateSELinux(label); err != nil {
			return fmt.Errorf("task %q: %w", task.Name, err)
		}
		found := false
		for i := range pr.Spec.TaskRunSpecs {
			if pr.Spec.TaskRunSpecs[i].PipelineTaskName == task.Name {
				pr.Spec.TaskRunSpecs[i].PodTemplate = template
				found = true
			}
		}
		if !found {
			pr.Spec.TaskRunSpecs = append(pr.Spec.TaskRunSpecs, tektonv1.PipelineTaskRunSpec{PipelineTaskName: task.Name, PodTemplate: template})
		}
	}
	return nil
}

func (d securityDefaults) validateUID(uid *int64) error {
	if uid != nil && (*uid <= 0 || (d.OpenShift && (*uid < d.UIDMin || *uid > d.UIDMax))) {
		return fmt.Errorf("user ID %d is outside the permitted non-root namespace range", *uid)
	}
	return nil
}

func (d securityDefaults) validateSELinux(options *corev1.SELinuxOptions) error {
	if options != nil && ((options.Type != "" && options.Type != "container_t") || (options.User != "" && options.User != "system_u") || (options.Role != "" && options.Role != "system_r") || (d.SELinuxLevel != "" && options.Level != d.SELinuxLevel)) {
		return fmt.Errorf("SELinux settings do not match the namespace's restricted container label")
	}
	return nil
}

func normalizePodSecurity(actual, expected *corev1.PodSecurityContext, defaults securityDefaults) error {
	if expected.RunAsUser == nil && actual.RunAsUser != nil {
		if err := defaults.validateUID(actual.RunAsUser); err != nil {
			return err
		}
		expected.RunAsUser = actual.RunAsUser
	}
	if expected.RunAsGroup == nil && actual.RunAsGroup != nil && *actual.RunAsGroup >= 0 {
		expected.RunAsGroup = actual.RunAsGroup
	}
	if expected.SELinuxOptions == nil && actual.SELinuxOptions != nil {
		if err := defaults.validateSELinux(actual.SELinuxOptions); err != nil {
			return err
		}
		expected.SELinuxOptions = actual.SELinuxOptions
	}
	return nil
}

func normalizeContainerSecurity(actual, expected *corev1.SecurityContext, defaults securityDefaults) error {
	if expected.RunAsUser == nil && actual.RunAsUser != nil {
		if err := defaults.validateUID(actual.RunAsUser); err != nil {
			return err
		}
		expected.RunAsUser = actual.RunAsUser
	}
	if expected.RunAsGroup == nil && actual.RunAsGroup != nil && *actual.RunAsGroup >= 0 {
		expected.RunAsGroup = actual.RunAsGroup
	}
	if expected.SELinuxOptions == nil && actual.SELinuxOptions != nil {
		if err := defaults.validateSELinux(actual.SELinuxOptions); err != nil {
			return err
		}
		expected.SELinuxOptions = actual.SELinuxOptions
	}
	if expected.SeccompProfile == nil && actual.SeccompProfile != nil && actual.SeccompProfile.Type == corev1.SeccompProfileTypeRuntimeDefault && actual.SeccompProfile.LocalhostProfile == nil {
		expected.SeccompProfile = actual.SeccompProfile
	}
	if expected.Privileged == nil && actual.Privileged != nil && !*actual.Privileged {
		expected.Privileged = actual.Privileged
	}
	if expected.RunAsNonRoot == nil && actual.RunAsNonRoot != nil && *actual.RunAsNonRoot {
		expected.RunAsNonRoot = actual.RunAsNonRoot
	}
	if expected.ProcMount == nil && actual.ProcMount != nil && *actual.ProcMount == corev1.DefaultProcMount {
		expected.ProcMount = actual.ProcMount
	}
	return nil
}

func (e *Execution) inspectTaskPods(ctx context.Context, pr *tektonv1.PipelineRun) error {
	pods, err := e.clients.Kube.CoreV1().Pods(pr.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "tekton.dev/pipelineRun=" + pr.Name})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		consumes := false
		for _, task := range pipelineTasks(pr) {
			if task.Name == pod.Labels["tekton.dev/pipelineTask"] && sourceConsumer(task, e.prepared.Workspace) {
				consumes = true
			}
		}
		if !consumes {
			continue
		}
		mounted := false
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == e.pvc.Name {
				mounted = true
			}
		}
		if !mounted || pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.FSGroup == nil || *pod.Spec.SecurityContext.FSGroup != e.contract.Group {
			return fmt.Errorf("task pod %s does not share the uploaded source volume and group", pod.Name)
		}
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
			if err := validateContainerSecurity(container.SecurityContext); err != nil {
				return fmt.Errorf("task pod %s container %s: %w", pod.Name, container.Name, err)
			}
			uid := pod.Spec.SecurityContext.RunAsUser
			label := pod.Spec.SecurityContext.SELinuxOptions
			if container.SecurityContext != nil {
				if container.SecurityContext.RunAsUser != nil {
					uid = container.SecurityContext.RunAsUser
				}
				if container.SecurityContext.SELinuxOptions != nil {
					label = container.SecurityContext.SELinuxOptions
				}
			}
			if e.security.OpenShift && label == nil {
				return fmt.Errorf("task pod %s container %s has no admitted SELinux label", pod.Name, container.Name)
			}
			if err := e.security.validateSELinux(label); err != nil {
				return fmt.Errorf("task pod %s container %s: %w", pod.Name, container.Name, err)
			}
			if e.security.OpenShift && uid == nil {
				return fmt.Errorf("task pod %s container %s has no admitted user ID", pod.Name, container.Name)
			}
			if err := e.security.validateUID(uid); err != nil {
				return fmt.Errorf("task pod %s container %s: %w", pod.Name, container.Name, err)
			}
		}
	}
	return nil
}

func (e *Execution) taskAdmissionFailure(ctx context.Context, pr *tektonv1.PipelineRun) error {
	tasks, err := e.clients.Tekton.TektonV1().TaskRuns(pr.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "tekton.dev/pipelineRun=" + pr.Name})
	if err != nil {
		return err
	}
	for _, task := range tasks.Items {
		condition := task.Status.GetCondition(apis.ConditionSucceeded)
		if condition != nil && condition.Status == corev1.ConditionFalse && condition.Reason == "PodAdmissionFailed" {
			return fmt.Errorf("TaskRun %s using service account %s was rejected by Pod admission: %s", task.Name, task.Spec.ServiceAccountName, condition.Message)
		}
	}
	return nil
}
