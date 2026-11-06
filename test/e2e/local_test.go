//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	"github.com/pipelines-as-code/tkn-local/pkg/localexecution"
	tektonpod "github.com/tektoncd/pipeline/pkg/apis/pipeline/pod"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

func TestLocalRun(t *testing.T) {
	assert.NilError(t, exec.CommandContext(t.Context(), "tkn", "version", "--component", "client").Run())
	options := &cluster.Options{}
	clients, err := cluster.New(options)
	assert.NilError(t, err)
	namespace, err := clients.Kube.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "tkn-local-e2e-"}}, metav1.CreateOptions{})
	assert.NilError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		assert.NilError(t, clients.Kube.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{}))
		assert.NilError(t, wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := clients.Kube.CoreV1().Namespaces().Get(ctx, namespace.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}))
	})
	scratch := t.TempDir()
	configPath, openShift := runnerConfig(t, clients, namespace, scratch)
	observed := observePods(t, clients, namespace.Name)
	binaryDir := filepath.Join(scratch, "bin")
	assert.NilError(t, os.Mkdir(binaryDir, 0o700))
	project, err := filepath.Abs("../..")
	assert.NilError(t, err)
	build := exec.CommandContext(t.Context(), "go", "build", "-o", filepath.Join(binaryDir, "tkn-local"), "./cmd/tkn-local")
	build.Dir = project
	output, err := build.CombinedOutput()
	assert.NilError(t, err, "%s", output)
	source := filepath.Join(scratch, "source")
	assert.NilError(t, os.MkdirAll(filepath.Join(source, ".tekton"), 0o700))
	fixture, err := os.ReadFile(filepath.Join(project, "pkg/localexecution/testdata/local.yaml"))
	assert.NilError(t, err)
	write := func(name, text string, mode os.FileMode) {
		t.Helper()
		assert.NilError(t, os.WriteFile(filepath.Join(source, name), []byte(text), mode))
	}
	write(".tekton/local.yaml", string(fixture), 0o600)
	write("local-input.txt", "committed\n", 0o600)
	write("local-deleted.txt", "deleted\n", 0o600)
	write(".gitignore", "local-ignored.txt\n", 0o600)
	write("local-executable.sh", "#!/bin/sh\nprintf 'executable source verified\\n'\n", 0o700)
	git := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "-c", "user.name=Test", "-c", "user.email=test@example.com"}, args...)...)
		cmd.Dir = source
		output, err := cmd.CombinedOutput()
		assert.NilError(t, err, "%s", output)
	}
	git("init", "-q")
	git("add", ".")
	git("commit", "-qm", "fixture")
	write("local-input.txt", "staged\n", 0o600)
	git("add", "local-input.txt")
	write("local-input.txt", "dirty working tree\n", 0o600)
	write("local-untracked.txt", "untracked source\n", 0o600)
	assert.NilError(t, os.Mkdir(filepath.Join(source, "nested"), 0o700))
	write("nested/input.txt", "nested source\n", 0o600)
	write("local-ignored.txt", "ignored\n", 0o600)
	assert.NilError(t, os.Remove(filepath.Join(source, "local-deleted.txt")))
	tests := []struct {
		name        string
		fail        bool
		keep        bool
		distinctUID bool
		interrupt   bool
	}{
		{name: "success"},
		{name: "failure", fail: true},
		{name: "keep resources", keep: true},
		{name: "distinct users", distinctUID: true},
		{name: "cancel without finally", interrupt: true, keep: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := &tektonv1.PipelineRun{}
			assert.NilError(t, yaml.Unmarshal(fixture, pr))
			var firstUID int64 = 1000
			if openShift {
				value, _, _ := strings.Cut(namespace.Annotations["openshift.io/sa.scc.uid-range"], "/")
				firstUID, err = strconv.ParseInt(value, 10, 64)
				assert.NilError(t, err)
			}
			if tt.distinctUID {
				for i, task := range pr.Spec.PipelineSpec.Tasks {
					pr.Spec.TaskRunSpecs = append(pr.Spec.TaskRunSpecs, tektonv1.PipelineTaskRunSpec{PipelineTaskName: task.Name, PodTemplate: &tektonpod.PodTemplate{SecurityContext: &corev1.PodSecurityContext{RunAsUser: ptr.To(firstUID + int64(i) + 1), RunAsGroup: ptr.To(int64(i + 1001))}}})
				}
			}
			if tt.interrupt {
				step := &pr.Spec.PipelineSpec.Tasks[1].TaskSpec.Steps[0]
				step.Script = strings.Replace(step.Script, "grep -q", "sleep 300\ngrep -q", 1)
				pr.Spec.PipelineSpec.Finally = []tektonv1.PipelineTask{{Name: "must-not-run", TaskSpec: &tektonv1.EmbeddedTask{TaskSpec: tektonv1.TaskSpec{Steps: []tektonv1.Step{{Name: "finally", Image: pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps[0].Image, Script: "#!/bin/sh\necho FINALLY_RAN\n"}}}}}}
			}
			definition, err := yaml.Marshal(pr)
			assert.NilError(t, err)
			write(".tekton/local.yaml", string(definition), 0o600)
			args := []string{
				"local", "run", "--pipeline", ".tekton/local.yaml", "--yes", "--timeout", "90s",
				"-p", fmt.Sprintf("expected-tracked=%x", sha256.Sum256([]byte("dirty working tree\n"))),
				"-p", fmt.Sprintf("expected-untracked=%x", sha256.Sum256([]byte("untracked source\n"))),
				"-p", fmt.Sprintf("force-failure=%t", tt.fail),
			}
			if tt.keep {
				args = append(args, "--keep-resources")
			}
			if tektonNamespace := os.Getenv("TKN_LOCAL_E2E_TEKTON_NAMESPACE"); tektonNamespace != "" {
				args = append(args, "--tekton-namespace", tektonNamespace)
			}
			cmd := exec.CommandContext(t.Context(), "tkn", args...)
			cmd.Dir = source
			cmd.Env = append(os.Environ(), "KUBECONFIG="+configPath, "PATH="+binaryDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			var output []byte
			if tt.interrupt {
				cmd.Path = filepath.Join(binaryDir, "tkn-local")
				cmd.Args = append([]string{cmd.Path}, args[1:]...)
				var buffer bytes.Buffer
				cmd.Stdout, cmd.Stderr = &buffer, &buffer
				assert.NilError(t, cmd.Start())
				ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
				pollErr := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
					pods, err := clients.Kube.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{LabelSelector: "tekton.dev/pipelineTask=verify-shared-source"})
					if err != nil {
						return false, err
					}
					for _, pod := range pods.Items {
						for _, container := range pod.Status.ContainerStatuses {
							if container.Name == "step-read-and-write" && container.State.Running != nil {
								return true, nil
							}
						}
					}
					return false, nil
				})
				cancel()
				assert.NilError(t, cmd.Process.Signal(os.Interrupt))
				err = cmd.Wait()
				output = buffer.Bytes()
				assert.NilError(t, pollErr, "%s", output)
			} else {
				output, err = cmd.CombinedOutput()
			}
			t.Logf("%s", output)
			if tt.interrupt {
				assert.Assert(t, err != nil, "interrupted command returned success")
				assert.Assert(t, strings.Contains(string(output), "context canceled"))
			} else if tt.fail {
				assert.Assert(t, err != nil, "failing pipeline returned success")
				assert.Assert(t, strings.Contains(string(output), "LOCAL_SOURCE_EXPECTED_FAILURE"))
			} else {
				assert.NilError(t, err)
			}
			if !tt.interrupt {
				assert.Assert(t, strings.Contains(string(output), "LOCAL_SOURCE_VERIFIED"))
			}
			match := regexp.MustCompile(`Created pending PipelineRun [^/]+/(\S+)`).FindSubmatch(output)
			assert.Equal(t, 2, len(match))
			name := string(match[1])
			if openShift {
				assertRestrictedPods(t, observed(), name, namespace)
			}
			if tt.distinctUID {
				for _, pod := range observed() {
					if pod.Labels["tekton.dev/pipelineRun"] != name {
						continue
					}
					for _, container := range pod.Spec.Containers {
						if !strings.HasPrefix(container.Name, "step-") {
							continue
						}
						uid := pod.Spec.SecurityContext.RunAsUser
						if container.SecurityContext != nil && container.SecurityContext.RunAsUser != nil {
							uid = container.SecurityContext.RunAsUser
						}
						expected := firstUID + 1
						if pod.Labels["tekton.dev/pipelineTask"] == "verify-shared-source" {
							expected++
						}
						assert.Assert(t, uid != nil && *uid == expected, "Pod %s did not preserve authored user", pod.Name)
					}
				}
			}
			if tt.keep {
				run, err := clients.Tekton.TektonV1().PipelineRuns(namespace.Name).Get(t.Context(), name, metav1.GetOptions{})
				assert.NilError(t, err)
				if tt.interrupt {
					assert.Equal(t, tektonv1.PipelineRunSpecStatus(tektonv1.PipelineRunSpecStatusCancelled), run.Spec.Status)
					for _, child := range run.Status.ChildReferences {
						assert.Assert(t, child.PipelineTaskName != "must-not-run", "cancellation started finally Task")
					}
				}
				_, err = clients.Kube.CoreV1().PersistentVolumeClaims(namespace.Name).Get(t.Context(), name+"-source", metav1.GetOptions{})
				assert.NilError(t, err)
				_, err = clients.Kube.CoreV1().Pods(namespace.Name).Get(t.Context(), name+"-upload", metav1.GetOptions{})
				assert.Assert(t, apierrors.IsNotFound(err), "uploader was retained")
				foreground := metav1.DeletePropagationForeground
				assert.NilError(t, clients.Tekton.TektonV1().PipelineRuns(namespace.Name).Delete(t.Context(), name, metav1.DeleteOptions{PropagationPolicy: &foreground}))
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			assert.NilError(t, wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
				selector := metav1.ListOptions{LabelSelector: localexecution.RunIDLabel + "=" + name}
				runs, err := clients.Tekton.TektonV1().PipelineRuns(namespace.Name).List(ctx, selector)
				if err != nil {
					return false, err
				}
				claims, err := clients.Kube.CoreV1().PersistentVolumeClaims(namespace.Name).List(ctx, selector)
				if err != nil {
					return false, err
				}
				pods, err := clients.Kube.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{})
				if err != nil {
					return false, err
				}
				tasks, err := clients.Tekton.TektonV1().TaskRuns(namespace.Name).List(ctx, metav1.ListOptions{LabelSelector: "tekton.dev/pipelineRun=" + name})
				if err != nil {
					return false, err
				}
				for _, task := range tasks.Items {
					if task.DeletionTimestamp == nil || !task.IsDone() {
						return false, nil
					}
				}
				return len(runs.Items)+len(claims.Items)+len(pods.Items) == 0, nil
			}))
		})
	}
}
