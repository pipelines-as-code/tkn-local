//go:build e2e

package e2e

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	"github.com/pipelines-as-code/tkn-local/pkg/localexecution"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
)

func TestLocalRun(t *testing.T) {
	assert.NilError(t, exec.CommandContext(t.Context(), "tkn", "version", "--component", "client").Run())
	options := &cluster.Options{}
	clients, err := cluster.New(options)
	assert.NilError(t, err)
	namespace, err := clients.Kube.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "tkn-local-e2e-"}}, metav1.CreateOptions{})
	assert.NilError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	kubeconfig, err := loader.RawConfig()
	assert.NilError(t, err)
	kubeconfig.Contexts[options.Context].Namespace = namespace.Name
	configPath := filepath.Join(scratch, "kubeconfig")
	assert.NilError(t, clientcmd.WriteToFile(kubeconfig, configPath))
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
	write("local-ignored.txt", "ignored\n", 0o600)
	assert.NilError(t, os.Remove(filepath.Join(source, "local-deleted.txt")))
	tests := []struct {
		name string
		fail bool
		keep bool
	}{
		{name: "success"},
		{name: "failure", fail: true},
		{name: "keep resources", keep: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{
				"local", "run", "--pipeline", ".tekton/local.yaml", "--yes", "--timeout", "90s",
				"-p", fmt.Sprintf("expected-tracked=%x", sha256.Sum256([]byte("dirty working tree\n"))),
				"-p", fmt.Sprintf("expected-untracked=%x", sha256.Sum256([]byte("untracked source\n"))),
				"-p", fmt.Sprintf("force-failure=%t", tt.fail),
			}
			if tt.keep {
				args = append(args, "--keep-resources")
			}
			cmd := exec.CommandContext(t.Context(), "tkn", args...)
			cmd.Dir = source
			cmd.Env = append(os.Environ(), "KUBECONFIG="+configPath, "PATH="+binaryDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			output, err := cmd.CombinedOutput()
			t.Logf("%s", output)
			if tt.fail {
				assert.Assert(t, err != nil, "failing pipeline returned success")
				assert.Assert(t, strings.Contains(string(output), "LOCAL_SOURCE_EXPECTED_FAILURE"))
			} else {
				assert.NilError(t, err)
			}
			assert.Assert(t, strings.Contains(string(output), "LOCAL_SOURCE_VERIFIED"))
			match := regexp.MustCompile(`Created pending PipelineRun [^/]+/(\S+)`).FindSubmatch(output)
			assert.Equal(t, 2, len(match))
			name := string(match[1])
			if tt.keep {
				_, err := clients.Tekton.TektonV1().PipelineRuns(namespace.Name).Get(t.Context(), name, metav1.GetOptions{})
				assert.NilError(t, err)
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
				return len(runs.Items)+len(claims.Items)+len(pods.Items)+len(tasks.Items) == 0, nil
			}))
		})
	}
}
