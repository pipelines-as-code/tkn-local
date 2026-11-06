package localexecution

import (
	"testing"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	tektonpod "github.com/tektoncd/pipeline/pkg/apis/pipeline/pod"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/yaml"
)

func TestNamespaceSecurityDefaults(t *testing.T) {
	tests := []struct {
		name      string
		uid       string
		groups    string
		wantGroup int64
		wantErr   string
	}{
		{name: "UID fallback", uid: "1000000000/10000", wantGroup: 1000000000},
		{name: "separate group allocation", uid: "1000000000/10000", groups: "2000000000/10000,2100000000/10000", wantGroup: 2000000000},
		{name: "dash allocation", uid: "1000000000-1000009999", groups: "2000000000-2000009999", wantGroup: 2000000000},
		{name: "malformed UID", uid: "no-range", wantErr: "invalid"},
		{name: "zero count", uid: "1000000000/0", wantErr: "invalid"},
		{name: "overflow", uid: "4294967290/10000", wantErr: "invalid"},
		{name: "malformed later group", uid: "1000000000/10000", groups: "2000000000/10000,bad", wantErr: "invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dev", Annotations: map[string]string{uidRangeAnnotation: tt.uid, mcsAnnotation: "s0:c1,c2"}}}
			if tt.groups != "" {
				ns.Annotations[groupsAnnotation] = tt.groups
			}
			defaults, err := namespaceSecurityDefaults(ns)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NilError(t, err)
			assert.Assert(t, defaults.OpenShift)
			assert.Equal(t, int64(1000000000), defaults.UIDMin)
			assert.Equal(t, int64(1000009999), defaults.UIDMax)
			assert.Equal(t, tt.wantGroup, defaults.Group)
		})
	}
}

func TestApplySecurityDefaults(t *testing.T) {
	tests := []struct {
		name          string
		openShift     bool
		authoredGroup *int64
		user          *int64
		label         *corev1.SELinuxOptions
		wantErr       string
	}{
		{name: "Kubernetes defaults"},
		{name: "OpenShift defers user assignment", openShift: true},
		{name: "preserve allowed user", openShift: true, user: ptr.To(int64(1000000001))},
		{name: "defer SELinux level assignment", openShift: true, label: &corev1.SELinuxOptions{Type: "container_t"}},
		{name: "preserve Kubernetes group", authoredGroup: ptr.To(int64(2000))},
		{name: "OpenShift group is only the allocation minimum", openShift: true, authoredGroup: ptr.To(int64(1000000001)), wantErr: "fsGroup"},
		{name: "incompatible UID", openShift: true, user: ptr.To(int64(65532)), wantErr: "user ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := &tektonv1.PipelineRun{}
			assert.NilError(t, yaml.Unmarshal([]byte(testPipeline), pr))
			pr.Spec.TaskRunTemplate.PodTemplate = nil
			pr.Spec.TaskRunSpecs = []tektonv1.PipelineTaskRunSpec{{PipelineTaskName: "test", ServiceAccountName: "builder", PodTemplate: &tektonpod.PodTemplate{SecurityContext: &corev1.PodSecurityContext{FSGroup: tt.authoredGroup, RunAsUser: tt.user, SELinuxOptions: tt.label}}}}
			before := pr.DeepCopy()
			defaults := securityDefaults{Group: defaultUser}
			if tt.openShift {
				defaults = securityDefaults{OpenShift: true, Group: 1000000000, UIDMin: 1000000000, UIDMax: 1000009999, SELinuxLevel: "s0:c1,c2"}
			}
			err := applySecurityDefaults(pr, "source", defaults)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NilError(t, err)
			spec := pr.GetTaskRunSpec("test")
			assert.Equal(t, "builder", spec.ServiceAccountName)
			group := defaults.Group
			if tt.authoredGroup != nil {
				group = *tt.authoredGroup
			}
			assert.Equal(t, group, *spec.PodTemplate.SecurityContext.FSGroup)
			assert.DeepEqual(t, tt.label, spec.PodTemplate.SecurityContext.SELinuxOptions)
			if tt.openShift {
				assert.DeepEqual(t, tt.user, spec.PodTemplate.SecurityContext.RunAsUser)
			} else {
				assert.Equal(t, defaultUser, *spec.PodTemplate.SecurityContext.RunAsUser)
			}
			assert.DeepEqual(t, before.Spec.TaskRunTemplate, pr.Spec.TaskRunTemplate)
		})
	}
}

func TestOpenShiftUploaderAdmission(t *testing.T) {
	defaults := securityDefaults{OpenShift: true, UIDMin: 1000000000, UIDMax: 1000009999, Group: 2000000000, SELinuxLevel: "s0:c1,c2"}
	tests := []struct {
		name    string
		mutate  func(*corev1.Pod)
		wantErr bool
	}{
		{name: "assigned UID differs from shared group", mutate: func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(1000000001))
			p.Spec.SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{Level: "s0:c1,c2"}
			p.Spec.Containers[0].SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
		}},
		{name: "root UID", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(0)) }},
		{name: "UID outside namespace", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsUser = ptr.To(int64(65532)) }},
		{name: "different namespace SELinux label", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{Level: "s0:c3,c4"} }},
		{name: "privileged SELinux type", wantErr: true, mutate: func(p *corev1.Pod) {
			p.Spec.SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{Type: "spc_t", Level: "s0:c1,c2"}
		}},
		{name: "service account changed", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.ServiceAccountName = "other" }},
		{name: "image changed", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.Containers[0].Image = "other" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requested := uploaderPod("upload", "dev", "source", &executionContract{Group: defaults.Group, OpenShift: true})
			assert.Assert(t, requested.Spec.SecurityContext.RunAsUser == nil)
			actual := requested.DeepCopy()
			tt.mutate(actual)
			err := validateUploader(actual, requested, defaults)
			if tt.wantErr {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestNamespaceReadPermissions(t *testing.T) {
	for _, openShift := range []bool{false, true} {
		t.Run(map[bool]string{false: "Kubernetes", true: "OpenShift"}[openShift], func(t *testing.T) {
			kube := kubefake.NewClientset()
			kube.PrependReactor("get", "namespaces", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "dev", nil)
			})
			if openShift {
				kube.Resources = []*metav1.APIResourceList{{GroupVersion: "security.openshift.io/v1", APIResources: []metav1.APIResource{{Name: "securitycontextconstraints", Kind: "SecurityContextConstraints"}}}}
			}
			defaults, err := loadSecurityDefaults(t.Context(), &cluster.Clients{Kube: kube}, "dev")
			if openShift {
				assert.ErrorContains(t, err, "read access to namespace")
			} else {
				assert.NilError(t, err)
				assert.Equal(t, defaultUser, defaults.Group)
			}
		})
	}
}

func TestInspectTaskPods(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*corev1.Pod)
		wantErr string
	}{
		{name: "restricted Task with separate UID and group", mutate: func(*corev1.Pod) {}},
		{name: "wrong source", wantErr: "source volume and group", mutate: func(p *corev1.Pod) { p.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other" }},
		{name: "wrong group", wantErr: "source volume and group", mutate: func(p *corev1.Pod) { p.Spec.SecurityContext.FSGroup = ptr.To(int64(1)) }},
		{name: "root step", wantErr: "root", mutate: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(0)) }},
		{name: "missing admitted UID", wantErr: "no admitted user ID", mutate: func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsUser = nil }},
		{name: "step outside allocation", wantErr: "namespace range", mutate: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(65532)) }},
		{name: "wrong SELinux label", wantErr: "SELinux", mutate: func(p *corev1.Pod) { p.Spec.SecurityContext.SELinuxOptions.Level = "s0:c3,c4" }},
		{name: "missing admitted SELinux label", wantErr: "no admitted SELinux label", mutate: func(p *corev1.Pod) { p.Spec.SecurityContext.SELinuxOptions = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, _, kube := testExecution(t)
			e.security = securityDefaults{OpenShift: true, UIDMin: 1000000000, UIDMax: 1000009999, Group: 2000000000, SELinuxLevel: "s0:c1,c2"}
			e.contract.Group = e.security.Group
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "task-pod", Namespace: "dev", Labels: map[string]string{"tekton.dev/pipelineRun": e.pipelineRun.Name, "tekton.dev/pipelineTask": "test"}},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{FSGroup: ptr.To(e.security.Group), RunAsUser: ptr.To(e.security.UIDMin + 1), SELinuxOptions: &corev1.SELinuxOptions{Level: e.security.SELinuxLevel}},
					Volumes:         []corev1.Volume{{Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: e.pvc.Name}}}},
					Containers:      []corev1.Container{{Name: "step", SecurityContext: &corev1.SecurityContext{}}},
				},
			}
			tt.mutate(pod)
			_, err := kube.CoreV1().Pods("dev").Create(t.Context(), pod, metav1.CreateOptions{})
			assert.NilError(t, err)
			err = e.inspectTaskPods(t.Context(), e.pipelineRun)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestTaskAdmissionFailure(t *testing.T) {
	e, tekton, _ := testExecution(t)
	task := &tektonv1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{Name: "rejected-task", Namespace: "dev", Labels: map[string]string{"tekton.dev/pipelineRun": e.pipelineRun.Name}},
		Spec:       tektonv1.TaskRunSpec{ServiceAccountName: "builder"},
	}
	task.Status.SetConditions([]apis.Condition{{Type: apis.ConditionSucceeded, Status: corev1.ConditionFalse, Reason: "PodAdmissionFailed", Message: "unable to validate against any security context constraint"}})
	_, err := tekton.TektonV1().TaskRuns("dev").Create(t.Context(), task, metav1.CreateOptions{})
	assert.NilError(t, err)
	err = e.taskAdmissionFailure(t.Context(), e.pipelineRun)
	assert.ErrorContains(t, err, "rejected-task using service account builder")
	assert.ErrorContains(t, err, "unable to validate against any security context constraint")
}
