package localexecution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	"github.com/pipelines-as-code/tkn-local/internal/iostreams"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	tektonfake "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/fake"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/yaml"
)

func testExecution(t *testing.T) (*Execution, *tektonfake.Clientset, *kubefake.Clientset) {
	t.Helper()
	pr := &tektonv1.PipelineRun{}
	assert.NilError(t, yaml.Unmarshal([]byte(testPipeline), pr))
	pr.Name = "tkn-local-test"
	pr.Namespace = "dev"
	pr.Labels = map[string]string{RunIDLabel: pr.Name}
	pr.Spec.Status = tektonv1.PipelineRunSpecStatusPending
	pr.Spec.Workspaces = []tektonv1.WorkspaceBinding{{Name: "source", PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pr.Name + "-source"}}}
	contract, err := admittedContract(pr, "source")
	assert.NilError(t, err)
	kube := kubefake.NewClientset()
	tekton := tektonfake.NewSimpleClientset()
	clients := &cluster.Clients{Kube: kube, Tekton: tekton}
	archive, err := os.CreateTemp(t.TempDir(), "snapshot")
	assert.NilError(t, err)
	_, err = archive.WriteString("captured source")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, archive.Close()) })
	streams := &iostreams.IOStreams{In: io.NopCloser(strings.NewReader("")), Out: io.Discard, ErrOut: io.Discard}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pr.Name + "-source", Namespace: pr.Namespace}}
	pod := uploaderPod(pr.Name+"-upload", pr.Namespace, pvc.Name, contract)
	execution := &Execution{clients: clients, streams: streams, options: RunOptions{Namespace: pr.Namespace, Context: "test", Kubeconfig: "config", Timeout: time.Second}, source: &Snapshot{archive: archive}, prepared: &Prepared{PipelineRun: pr, Workspace: "source"}, contract: contract, pipelineRun: pr, pvc: pvc, pod: pod}
	return execution, tekton, kube
}

func TestExecutionLifecycle(t *testing.T) {
	tests := []struct {
		name            string
		uploadError     bool
		pvcError        bool
		admissionChange bool
		pendingChange   bool
		pipelineFailure bool
		logFailure      bool
		cancel          bool
		keep            bool
		wantErr         string
		wantResume      bool
	}{
		{name: "success", wantResume: true},
		{name: "failed pipeline", pipelineFailure: true, wantErr: "pipeline failed", wantResume: true},
		{name: "failed log transport", logFailure: true, wantErr: "log transport failed", wantResume: true},
		{name: "failed upload never resumes", uploadError: true, wantErr: "upload failed"},
		{name: "partial creation cleanup", pvcError: true, wantErr: "PVC creation failed"},
		{name: "admission changed before upload", admissionChange: true, wantErr: "after preview"},
		{name: "pending changed during upload", pendingChange: true, wantErr: "refusing to resume"},
		{name: "cancel running pipeline", cancel: true, wantErr: "context", wantResume: true},
		{name: "retained successful run", keep: true, wantResume: true},
		{name: "retained upload failure stays stopped", keep: true, uploadError: true, wantErr: "upload failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execution, tekton, kube := testExecution(t)
			execution.options.KeepResources = tt.keep
			var resumed, uploaded bool
			var logsExited atomic.Bool
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tekton.PrependReactor("create", "pipelineruns", func(action ktesting.Action) (bool, runtime.Object, error) {
				typedAction, ok := action.(ktesting.CreateAction)
				assert.Assert(t, ok)
				pr, ok := typedAction.GetObject().(*tektonv1.PipelineRun)
				assert.Assert(t, ok)
				assert.Equal(t, tektonv1.PipelineRunSpecStatus(tektonv1.PipelineRunSpecStatusPending), pr.Spec.Status)
				pr.UID = types.UID("run-uid")
				if tt.admissionChange {
					pr.Spec.TaskRunTemplate.ServiceAccountName = "changed"
				}
				return false, nil, nil
			})
			kube.PrependReactor("create", "persistentvolumeclaims", func(action ktesting.Action) (bool, runtime.Object, error) {
				if tt.pvcError {
					return true, &corev1.PersistentVolumeClaim{}, fmt.Errorf("PVC creation failed")
				}
				typedAction, ok := action.(ktesting.CreateAction)
				assert.Assert(t, ok)
				pvc, ok := typedAction.GetObject().(*corev1.PersistentVolumeClaim)
				assert.Assert(t, ok)
				assert.Assert(t, ownedBy(pvc.OwnerReferences, "run-uid"))
				pvc.UID = "pvc-uid"
				pvc.Status.Phase = corev1.ClaimBound
				return false, nil, nil
			})
			kube.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				typedAction, ok := action.(ktesting.CreateAction)
				assert.Assert(t, ok)
				pod, ok := typedAction.GetObject().(*corev1.Pod)
				assert.Assert(t, ok)
				assert.Assert(t, ownedBy(pod.OwnerReferences, "run-uid"))
				pod.UID = "pod-uid"
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionTrue}}
				return false, nil, nil
			})
			tekton.PrependReactor("update", "pipelineruns", func(action ktesting.Action) (bool, runtime.Object, error) {
				typedAction, ok := action.(ktesting.UpdateAction)
				assert.Assert(t, ok)
				pr, ok := typedAction.GetObject().(*tektonv1.PipelineRun)
				assert.Assert(t, ok)
				if pr.Spec.Status == "" {
					assert.Assert(t, uploaded)
					_, err := kube.CoreV1().Pods("dev").Get(ctx, execution.pod.Name, metav1.GetOptions{})
					assert.Assert(t, apierrors.IsNotFound(err), "uploader must be gone before resume")
					resumed = true
					if !tt.cancel {
						status := corev1.ConditionTrue
						message := "success"
						if tt.pipelineFailure {
							status = corev1.ConditionFalse
							message = "pipeline failed"
						}
						pr.Status.SetConditions([]apis.Condition{{Type: apis.ConditionSucceeded, Status: status, Reason: message, Message: message}})
					}
				}
				return false, nil, nil
			})
			tekton.PrependReactor("delete", "pipelineruns", func(action ktesting.Action) (bool, runtime.Object, error) {
				if resumed {
					assert.Assert(t, logsExited.Load(), "logs must exit before cleanup")
				}
				typedAction, ok := action.(ktesting.DeleteAction)
				assert.Assert(t, ok)
				options := typedAction.GetDeleteOptions()
				assert.Equal(t, types.UID("run-uid"), *options.Preconditions.UID)
				assert.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
				return false, nil, nil
			})
			execution.uploadSource = func(ctx context.Context, _ *corev1.Pod, archive io.Reader) error {
				current, err := tekton.TektonV1().PipelineRuns("dev").Get(ctx, execution.pipelineRun.Name, metav1.GetOptions{})
				assert.NilError(t, err)
				assert.Equal(t, tektonv1.PipelineRunSpecStatus(tektonv1.PipelineRunSpecStatusPending), current.Spec.Status)
				data, err := io.ReadAll(archive)
				assert.NilError(t, err)
				assert.Equal(t, "captured source", string(data))
				uploaded = true
				if tt.uploadError {
					return fmt.Errorf("upload failed")
				}
				if tt.pendingChange {
					current.Spec.TaskRunTemplate.ServiceAccountName = "changed"
					_, err = tekton.TektonV1().PipelineRuns("dev").Update(ctx, current, metav1.UpdateOptions{})
					assert.NilError(t, err)
				}
				return nil
			}
			execution.followLogs = func(ctx context.Context, _ string) error {
				defer logsExited.Store(true)
				if tt.cancel {
					cancel()
					<-ctx.Done()
					return ctx.Err()
				}
				if tt.logFailure {
					return fmt.Errorf("log transport failed")
				}
				return nil
			}
			err := execution.Run(ctx)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				if tt.cancel {
					assert.Assert(t, errors.Is(err, ctx.Err()))
				}
			} else {
				assert.NilError(t, err)
			}
			assert.Equal(t, tt.wantResume, resumed)
			assert.Equal(t, !tt.admissionChange && !tt.pvcError, uploaded)
			pods, err := kube.CoreV1().Pods("dev").List(t.Context(), metav1.ListOptions{})
			assert.NilError(t, err)
			assert.Equal(t, 0, len(pods.Items))
			runs, err := tekton.TektonV1().PipelineRuns("dev").List(t.Context(), metav1.ListOptions{})
			assert.NilError(t, err)
			claims, err := kube.CoreV1().PersistentVolumeClaims("dev").List(t.Context(), metav1.ListOptions{})
			assert.NilError(t, err)
			if tt.keep {
				assert.Equal(t, 1, len(runs.Items))
				assert.Equal(t, 1, len(claims.Items))
				if tt.uploadError {
					assert.Equal(t, tektonv1.PipelineRunSpecStatus(tektonv1.PipelineRunSpecStatusCancelled), runs.Items[0].Spec.Status)
				}
			} else {
				assert.Equal(t, 0, len(runs.Items))
				assert.Equal(t, 0, len(claims.Items))
			}
		})
	}
}

func TestCleanupUIDPreconditions(t *testing.T) {
	execution, tekton, kube := testExecution(t)
	original := execution.pipelineRun.DeepCopy()
	original.UID = "original"
	replacement := original.DeepCopy()
	replacement.UID = "replacement"
	_, err := tekton.TektonV1().PipelineRuns("dev").Create(t.Context(), replacement, metav1.CreateOptions{})
	assert.NilError(t, err)
	pvc := execution.pvc.DeepCopy()
	pvc.UID = "source"
	_, err = kube.CoreV1().PersistentVolumeClaims("dev").Create(t.Context(), pvc, metav1.CreateOptions{})
	assert.NilError(t, err)
	tekton.PrependReactor("delete", "pipelineruns", func(action ktesting.Action) (bool, runtime.Object, error) {
		typedAction, ok := action.(ktesting.DeleteAction)
		assert.Assert(t, ok)
		options := typedAction.GetDeleteOptions()
		assert.Equal(t, original.UID, *options.Preconditions.UID)
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "tekton.dev", Resource: "pipelineruns"}, replacement.Name, fmt.Errorf("UID mismatch"))
	})
	err = execution.cleanup(t.Context(), original, pvc, nil, false)
	assert.ErrorContains(t, err, "UID mismatch")
	current, err := tekton.TektonV1().PipelineRuns("dev").Get(t.Context(), replacement.Name, metav1.GetOptions{})
	assert.NilError(t, err)
	assert.Equal(t, replacement.UID, current.UID)
	_, err = kube.CoreV1().PersistentVolumeClaims("dev").Get(t.Context(), pvc.Name, metav1.GetOptions{})
	assert.NilError(t, err)
}

func TestCleanupRetainedTaskMetadata(t *testing.T) {
	for _, runningPod := range []bool{false, true} {
		t.Run(fmt.Sprintf("running Pod %t", runningPod), func(t *testing.T) {
			e, tekton, kube := testExecution(t)
			e.pipelineRun.UID = "run-uid"
			task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "task", Namespace: "dev", Labels: map[string]string{"tekton.dev/pipelineRun": e.pipelineRun.Name}, OwnerReferences: []metav1.OwnerReference{{UID: e.pipelineRun.UID}}, Finalizers: []string{"results.tekton.dev/taskrun"}}}
			task.Status.SetConditions([]apis.Condition{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}})
			_, err := tekton.TektonV1().TaskRuns("dev").Create(t.Context(), task, metav1.CreateOptions{})
			assert.NilError(t, err)
			if runningPod {
				_, err = kube.CoreV1().Pods("dev").Create(t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "task-pod", Namespace: "dev", Labels: task.Labels}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}, metav1.CreateOptions{})
				assert.NilError(t, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			err = e.waitTaskPods(ctx, e.pipelineRun, true)
			if runningPod {
				assert.ErrorContains(t, err, "context deadline exceeded")
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestUploaderAdmission(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*corev1.Pod)
		wantErr bool
	}{
		{name: "standard defaults", mutate: func(p *corev1.Pod) {
			p.Spec.DNSPolicy = corev1.DNSClusterFirst
			p.Spec.SchedulerName = "default-scheduler"
			p.Spec.ServiceAccountName = "default"
			p.Spec.DeprecatedServiceAccount = "default"
			p.Spec.TerminationGracePeriodSeconds = ptr.To(int64(30))
			p.Spec.EnableServiceLinks = ptr.To(true)
			p.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent
			p.Spec.Containers[0].TerminationMessagePath = "/dev/termination-log"
			p.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
			p.Spec.Tolerations = []corev1.Toleration{{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(300))}}
		}},
		{name: "injected container", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar"}) }},
		{name: "privileged", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.Privileged = ptr.To(true) }},
		{name: "different fsGroup", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.SecurityContext.FSGroup = ptr.To(int64(1234)) }},
		{name: "service account token", wantErr: true, mutate: func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = ptr.To(true) }},
		{name: "memory pressure default", mutate: func(p *corev1.Pod) {
			p.Spec.Tolerations = []corev1.Toleration{{Key: "node.kubernetes.io/memory-pressure", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
		}},
		{name: "arbitrary placement", wantErr: true, mutate: func(p *corev1.Pod) {
			p.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requested := uploaderPod("upload", "dev", "source", &executionContract{Group: 65532})
			actual := requested.DeepCopy()
			tt.mutate(actual)
			err := validateUploader(actual, requested, securityDefaults{})
			if tt.wantErr {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}
