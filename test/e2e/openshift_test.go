//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	"gotest.tools/v3/assert"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
)

func runnerConfig(t *testing.T, clients *cluster.Clients, namespace *corev1.Namespace, scratch string) (string, bool) {
	t.Helper()
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	config, err := loader.RawConfig()
	assert.NilError(t, err)
	config.Contexts[config.CurrentContext].Namespace = namespace.Name
	openShift := os.Getenv("TKN_LOCAL_E2E_OPENSHIFT") == "1"
	if openShift {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		assert.NilError(t, wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
			current, err := clients.Kube.CoreV1().Namespaces().Get(ctx, namespace.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			*namespace = *current
			return namespace.Annotations["openshift.io/sa.scc.uid-range"] != "", nil
		}))
		account := "tkn-local-runner"
		_, err := clients.Kube.CoreV1().ServiceAccounts(namespace.Name).Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: account}}, metav1.CreateOptions{})
		assert.NilError(t, err)
		rules := []rbacv1.PolicyRule{
			{APIGroups: []string{"tekton.dev"}, Resources: []string{"pipelineruns"}, Verbs: []string{"create", "get", "list", "watch", "update", "delete"}},
			{APIGroups: []string{"tekton.dev"}, Resources: []string{"pipelineruns/finalizers"}, Verbs: []string{"update"}},
			{APIGroups: []string{"tekton.dev"}, Resources: []string{"taskruns"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"pods", "persistentvolumeclaims"}, Verbs: []string{"create", "get", "list", "watch", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"get", "create"}},
			{APIGroups: []string{""}, Resources: []string{"pods/log", "serviceaccounts"}, Verbs: []string{"get"}},
		}
		_, err = clients.Kube.RbacV1().Roles(namespace.Name).Create(t.Context(), &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: account}, Rules: rules}, metav1.CreateOptions{})
		assert.NilError(t, err)
		subjects := []rbacv1.Subject{{Kind: "ServiceAccount", Name: account, Namespace: namespace.Name}}
		_, err = clients.Kube.RbacV1().RoleBindings(namespace.Name).Create(t.Context(), &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: account}, Subjects: subjects, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: account}}, metav1.CreateOptions{})
		assert.NilError(t, err)
		readRules := []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{namespace.Name}, Verbs: []string{"get"}},
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"list"}},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"storageclasses"}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{"feature-flags"}, Verbs: []string{"get"}},
		}
		roleName := namespace.Name + "-discovery"
		_, err = clients.Kube.RbacV1().ClusterRoles().Create(t.Context(), &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: roleName}, Rules: readRules}, metav1.CreateOptions{})
		assert.NilError(t, err)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			assert.NilError(t, clients.Kube.RbacV1().ClusterRoles().Delete(ctx, roleName, metav1.DeleteOptions{}))
		})
		_, err = clients.Kube.RbacV1().ClusterRoleBindings().Create(t.Context(), &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: roleName}, Subjects: subjects, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName}}, metav1.CreateOptions{})
		assert.NilError(t, err)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			assert.NilError(t, clients.Kube.RbacV1().ClusterRoleBindings().Delete(ctx, roleName, metav1.DeleteOptions{}))
		})
		token, err := clients.Kube.CoreV1().ServiceAccounts(namespace.Name).CreateToken(t.Context(), account, &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: ptr.To(int64(3600))}}, metav1.CreateOptions{})
		assert.NilError(t, err)
		current := config.Contexts[config.CurrentContext].DeepCopy()
		current.AuthInfo = account
		config.AuthInfos = map[string]*clientcmdapi.AuthInfo{account: {Token: token.Status.Token}}
		config.Contexts = map[string]*clientcmdapi.Context{account: current}
		config.CurrentContext = account
	}
	filename := filepath.Join(scratch, "kubeconfig")
	assert.NilError(t, clientcmd.WriteToFile(config, filename))
	if openShift {
		runner, err := cluster.New(&cluster.Options{Kubeconfig: filename})
		assert.NilError(t, err)
		for _, scc := range []string{"anyuid", "privileged"} {
			review, err := runner.Kube.AuthorizationV1().SelfSubjectAccessReviews().Create(t.Context(), &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{Group: "security.openshift.io", Resource: "securitycontextconstraints", Name: scc, Verb: "use", Namespace: namespace.Name}}}, metav1.CreateOptions{})
			assert.NilError(t, err)
			assert.Assert(t, !review.Status.Allowed, "E2E runner can use %s", scc)
		}
	}
	return filename, openShift
}

func observePods(t *testing.T, clients *cluster.Clients, namespace string) func() []corev1.Pod {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	watcher, err := clients.Kube.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{})
	assert.NilError(t, err)
	var mutex sync.Mutex
	pods := map[string]corev1.Pod{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range watcher.ResultChan() {
			if pod, ok := event.Object.(*corev1.Pod); ok {
				mutex.Lock()
				pods[string(pod.UID)] = *pod.DeepCopy()
				mutex.Unlock()
			}
		}
	}()
	t.Cleanup(func() { cancel(); watcher.Stop(); <-done })
	return func() []corev1.Pod {
		mutex.Lock()
		defer mutex.Unlock()
		result := make([]corev1.Pod, 0, len(pods))
		for _, pod := range pods {
			result = append(result, *pod.DeepCopy())
		}
		return result
	}
}

func assertRestrictedPods(t *testing.T, observed []corev1.Pod, run string, namespace *corev1.Namespace, wantUploader, wantTasks int) {
	t.Helper()
	uploader, tasks := 0, 0
	for _, pod := range observed {
		if pod.Name != run+"-upload" && pod.Labels["tekton.dev/pipelineRun"] != run {
			continue
		}
		if pod.Name == run+"-upload" {
			uploader++
		} else if pod.Labels["tekton.dev/taskRun"] != "" {
			tasks++
		} else {
			continue
		}
		scc := pod.Annotations["openshift.io/scc"]
		assert.Assert(t, scc == "restricted" || scc == "restricted-v2" || scc == "restricted-v3", "Pod %s used SCC %q", pod.Name, scc)
		assert.Assert(t, pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.FSGroup != nil)
		for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
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
			assert.Assert(t, uid != nil && *uid > 0, "Pod %s container %s has no non-root UID", pod.Name, container.Name)
			assert.Assert(t, label != nil && label.Level == namespace.Annotations["openshift.io/sa.scc.mcs"], "Pod %s has incompatible SELinux labels", pod.Name)
		}
	}
	assert.Equal(t, wantUploader, uploader)
	assert.Equal(t, wantTasks, tasks)
}
