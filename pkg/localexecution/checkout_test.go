package localexecution

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/yaml"
)

func checkoutPipeline(t *testing.T) *tektonv1.PipelineRun {
	t.Helper()
	pr := &tektonv1.PipelineRun{}
	assert.NilError(t, yaml.Unmarshal([]byte(testPipeline), pr))
	clone := pr.Spec.PipelineSpec.Tasks[0].DeepCopy()
	clone.Name = "git-clone"
	clone.TaskSpec.Steps[0].Name = "clone"
	clone.TaskSpec.Steps[0].Script = "git clone https://example.invalid/repo ."
	clone.TaskSpec.Params = []tektonv1.ParamSpec{{Name: "url", Type: tektonv1.ParamTypeString}}
	clone.Params = []tektonv1.Param{{Name: "url", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: "https://example.invalid/repo"}}}
	clone.TaskSpec.Results = []tektonv1.TaskResult{{Name: "commit"}, {Name: "url"}, {Name: "committer-date"}, {Name: "unused-custom"}}
	pr.Spec.PipelineSpec.Tasks[0].RunAfter = []string{clone.Name}
	pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps[0].Env = []corev1.EnvVar{{Name: "COMMIT", Value: "$(tasks.git-clone.results.commit)"}}
	pr.Spec.PipelineSpec.Tasks = append([]tektonv1.PipelineTask{*clone}, pr.Spec.PipelineSpec.Tasks...)
	return pr
}

func TestCheckoutReplacement(t *testing.T) {
	tests := []struct {
		name     string
		override string
		known    map[string]bool
		setup    func(*tektonv1.PipelineRun)
		wantErr  string
		skipped  bool
	}{
		{name: "automatic"},
		{name: "custom task override", override: "fetch-source", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].Name = "fetch-source"
			pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps = append(pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps, tektonv1.Step{Name: "custom", Image: "custom"})
			pr.Spec.PipelineSpec.Tasks[1].RunAfter = []string{"fetch-source"}
			pr.Spec.PipelineSpec.Tasks[1].TaskSpec.Steps[0].Env[0].Value = "$(tasks.fetch-source.results.commit)"
		}},
		{name: "unknown override", override: "missing", wantErr: "does not name a main Pipeline task"},
		{name: "multiple candidates", setup: func(pr *tektonv1.PipelineRun) {
			other := pr.Spec.PipelineSpec.Tasks[0].DeepCopy()
			other.Name = "fetch-source"
			pr.Spec.PipelineSpec.Tasks = append(pr.Spec.PipelineSpec.Tasks, *other)
		}, known: map[string]bool{"fetch-source": true}, wantErr: "multiple source checkout tasks"},
		{name: "task execution settings preserved", setup: func(pr *tektonv1.PipelineRun) {
			clone := &pr.Spec.PipelineSpec.Tasks[0]
			clone.When = tektonv1.WhenExpressions{{Input: "CI", Operator: selection.In, Values: []string{"CI"}}}
			clone.Retries = 2
			clone.Timeout = &metav1.Duration{Duration: time.Minute}
			clone.RunAfter = []string{"prepare"}
			pr.Spec.PipelineSpec.Tasks = append(pr.Spec.PipelineSpec.Tasks, tektonv1.PipelineTask{Name: "prepare", TaskSpec: &tektonv1.EmbeddedTask{TaskSpec: tektonv1.TaskSpec{Steps: []tektonv1.Step{{Name: "prepare", Image: uploaderImage, Script: "echo ready"}}}}})
			pr.Spec.TaskRunSpecs = []tektonv1.PipelineTaskRunSpec{{PipelineTaskName: "git-clone", ServiceAccountName: "source-account"}}
		}},
		{name: "combined task needs override", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps = append(pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps, tektonv1.Step{Name: "build"})
		}, wantErr: "entire task can be replaced"},
		{name: "source subpath", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].Workspaces[0].SubPath = "nested"
		}, wantErr: "at its root"},
		{name: "checkout subdirectory", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Params = append(pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Params, tektonv1.ParamSpec{Name: "subdirectory", Default: &tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: "nested"}})
		}, wantErr: "source subdirectory"},
		{name: "override requires source", override: "git-clone", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].Workspaces = nil
		}, wantErr: "exactly one source workspace"},
		{name: "finally checkout", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Finally = []tektonv1.PipelineTask{pr.Spec.PipelineSpec.Tasks[0]}
			pr.Spec.PipelineSpec.Tasks = pr.Spec.PipelineSpec.Tasks[1:]
		}, wantErr: "in finally"},
		{name: "matrix checkout", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].Matrix = &tektonv1.Matrix{}
		}, wantErr: "matrix"},
		{name: "consumed custom result", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[1].TaskSpec.Steps[0].Env[0].Value = "$(tasks.git-clone.results.unused-custom)"
		}, wantErr: "cannot be supplied"},
		{name: "consumed bracket notation result", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[1].TaskSpec.Steps[0].Env[0].Value = "$(tasks.git-clone.results['unused-custom'])"
		}, wantErr: "cannot be supplied"},
		{name: "consumed array result", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Results[0].Type = tektonv1.ResultsTypeArray
		}, wantErr: "cannot be supplied"},
		{name: "consumed undeclared result", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[1].TaskSpec.Steps[0].Env[0].Value = "$(tasks.git-clone.results.missing)"
		}, wantErr: "unsupported consumed results: missing"},
		{name: "URL result needs parameter", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Params = nil
			pr.Spec.PipelineSpec.Tasks[1].TaskSpec.Steps[0].Env[0].Value = "$(tasks.git-clone.results.url)"
		}, wantErr: "cannot be supplied"},
		{name: "Pipeline result dependency", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Results = []tektonv1.PipelineResult{{Name: "custom", Value: tektonv1.ResultValue{Type: tektonv1.ParamTypeString, StringVal: "$(tasks.git-clone.results.unused-custom)"}}}
		}, wantErr: "cannot be supplied"},
		{name: "author gated checkout", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].When = tektonv1.WhenExpressions{{Input: "local", Operator: selection.In, Values: []string{"CI"}}}
			pr.Spec.PipelineSpec.Tasks[1].TaskSpec.Steps[0].Env = nil
		}, skipped: true},
		{name: "gated checkout result still unavailable", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].When = tektonv1.WhenExpressions{{Input: "local", Operator: selection.In, Values: []string{"CI"}}}
		}, wantErr: "skipped checkout with consumed results"},
		{name: "additional checkout refused", override: "git-clone", setup: func(pr *tektonv1.PipelineRun) {
			other := pr.Spec.PipelineSpec.Tasks[0].DeepCopy()
			other.Name = "another-checkout"
			pr.Spec.PipelineSpec.Tasks = append(pr.Spec.PipelineSpec.Tasks, *other)
		}, wantErr: "performs checkout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := checkoutPipeline(t)
			if tt.setup != nil {
				tt.setup(pr)
			}
			original := pr.DeepCopy()
			replacement, err := replaceCheckout(pr, &Snapshot{Head: strings.Repeat("a", 40), CommitterDate: "1700000000"}, "source", tt.override, tt.known)
			if err == nil {
				_, err = validatePipeline(pr, "source", replacement)
			}
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NilError(t, err)
			if tt.skipped {
				assert.Assert(t, replacement == nil)
				assert.DeepEqual(t, original.Spec, pr.Spec)
				return
			}
			assert.Assert(t, replacement != nil)
			assert.Equal(t, "local-source", pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps[0].Name)
			assert.Equal(t, 3, len(pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Results))
			assert.DeepEqual(t, original.Spec.PipelineSpec.Tasks[1:], pr.Spec.PipelineSpec.Tasks[1:])
			assert.DeepEqual(t, original.Spec.TaskRunTemplate, pr.Spec.TaskRunTemplate)
			assert.DeepEqual(t, original.Spec.TaskRunSpecs, pr.Spec.TaskRunSpecs)
			originalTask := original.Spec.PipelineSpec.Tasks[0].DeepCopy()
			originalTask.TaskSpec = pr.Spec.PipelineSpec.Tasks[0].TaskSpec
			originalTask.Workspaces = pr.Spec.PipelineSpec.Tasks[0].Workspaces
			assert.DeepEqual(t, *originalTask, pr.Spec.PipelineSpec.Tasks[0])
		})
	}
}

func TestLocalCheckoutResults(t *testing.T) {
	pr := checkoutPipeline(t)
	url := "https://example.invalid/$(touch SHOULD_NOT_EXIST)"
	pr.Spec.PipelineSpec.Tasks[0].Params[0].Value.StringVal = url
	replacement, err := replaceCheckout(pr, &Snapshot{Head: strings.Repeat("a", 40), CommitterDate: "1700000000"}, "source", "", nil)
	assert.NilError(t, err)
	step := replacement.task.TaskSpec.Steps[0]
	root := t.TempDir()
	command := exec.CommandContext(t.Context(), "sh", "-c", step.Script)
	command.Dir = root
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "LOCAL_SOURCE=" + root}
	for i, value := range []string{strings.Repeat("a", 40), url, "1700000000"} {
		command.Env = append(command.Env, "LOCAL_RESULT_"+strconv.Itoa(i+1)+"="+value, "RESULT_PATH_"+strconv.Itoa(i+1)+"="+filepath.Join(root, replacement.task.TaskSpec.Results[i].Name))
	}
	output, err := command.CombinedOutput()
	assert.NilError(t, err, "%s", output)
	for name, want := range map[string]string{"commit": strings.Repeat("a", 40), "url": url, "committer-date": "1700000000"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		assert.NilError(t, err)
		assert.Equal(t, want, string(data))
	}
	_, err = os.Stat(filepath.Join(root, "SHOULD_NOT_EXIST"))
	assert.Assert(t, os.IsNotExist(err))
}

func TestCheckoutAdmission(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tektonv1.PipelineTask)
	}{
		{name: "checkout reintroduced", mutate: func(task *tektonv1.PipelineTask) { task.TaskSpec.Steps[0].Script = "git clone remote ." }},
		{name: "sidecar added", mutate: func(task *tektonv1.PipelineTask) {
			task.TaskSpec.Sidecars = []tektonv1.Sidecar{{Name: "clone", Image: "custom"}}
		}},
		{name: "source remapped", mutate: func(task *tektonv1.PipelineTask) { task.Workspaces[0].Workspace = "other" }},
		{name: "condition changed", mutate: func(task *tektonv1.PipelineTask) {
			task.When = tektonv1.WhenExpressions{{Input: "false", Operator: selection.In, Values: []string{"true"}}}
		}},
		{name: "result redirected", mutate: func(task *tektonv1.PipelineTask) {
			task.TaskSpec.Results[0].Value = &tektonv1.ResultValue{StringVal: "other"}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := checkoutPipeline(t)
			replacement, err := replaceCheckout(pr, &Snapshot{Head: "abc", CommitterDate: "1700000000"}, "source", "", nil)
			assert.NilError(t, err)
			pr.Spec.PipelineSpec.Tasks[0].SetDefaults(context.Background())
			assert.NilError(t, replacement.validate(pr))
			tt.mutate(&pr.Spec.PipelineSpec.Tasks[0])
			assert.ErrorContains(t, replacement.validate(pr), "changed during admission")
		})
	}
}
