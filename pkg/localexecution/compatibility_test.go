package localexecution

import (
	"testing"

	tektonpod "github.com/tektoncd/pipeline/pkg/apis/pipeline/pod"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

func TestPipelineCompatibility(t *testing.T) {
	gated := func(pr *tektonv1.PipelineRun) {
		pr.Spec.PipelineSpec.Params = []tektonv1.ParamSpec{{Name: "local", Type: tektonv1.ParamTypeString}}
		pr.Spec.Params = []tektonv1.Param{{Name: "local", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: "true"}}}
		task := &pr.Spec.PipelineSpec.Tasks[0]
		task.Name = "git-clone"
		task.When = tektonv1.WhenExpressions{{Input: "$(params.local)", Operator: selection.In, Values: []string{"false"}}}
	}
	tests := []struct {
		name    string
		setup   func(*tektonv1.PipelineRun)
		wantErr string
	}{
		{name: "author gated checkout", setup: gated},
		{name: "checkout result dependency", setup: func(pr *tektonv1.PipelineRun) {
			gated(pr)
			consumer := pr.Spec.PipelineSpec.Tasks[0].DeepCopy()
			consumer.Name = "consumer"
			consumer.Params = []tektonv1.Param{{Name: "commit", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: "$(tasks.git-clone.results.commit)"}}}
			pr.Spec.PipelineSpec.Tasks = append(pr.Spec.PipelineSpec.Tasks, *consumer)
		}, wantErr: "consumed results"},
		{name: "finally checkout result dependency", setup: func(pr *tektonv1.PipelineRun) {
			gated(pr)
			checkout := pr.Spec.PipelineSpec.Tasks[0].DeepCopy()
			pr.Spec.PipelineSpec.Tasks[0].Name = "normal"
			pr.Spec.PipelineSpec.Tasks[0].When = nil
			pr.Spec.PipelineSpec.Finally = []tektonv1.PipelineTask{*checkout}
			pr.Spec.PipelineSpec.Results = []tektonv1.PipelineResult{{Name: "commit", Value: tektonv1.ResultValue{Type: tektonv1.ParamTypeString, StringVal: "$(finally.git-clone.results.commit)"}}}
		}, wantErr: "consumed results"},
		{name: "unbound credentials", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Workspaces = append(pr.Spec.PipelineSpec.Workspaces, tektonv1.PipelineWorkspaceDeclaration{Name: "credentials"})
		}, wantErr: "unbound"},
		{name: "privileged step", setup: func(pr *tektonv1.PipelineRun) {
			pr.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps[0].SecurityContext = &corev1.SecurityContext{Privileged: ptr.To(true)}
		}, wantErr: "privileged"},
		{name: "source mapping subpath", setup: func(pr *tektonv1.PipelineRun) { pr.Spec.PipelineSpec.Tasks[0].Workspaces[0].SubPath = "sub" }, wantErr: "subPath"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := &tektonv1.PipelineRun{}
			assert.NilError(t, yaml.Unmarshal([]byte(testPipeline), pr))
			tt.setup(pr)
			_, err := validatePipeline(pr, "source", nil)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestSourcePermissionContract(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*tektonv1.PipelineRun)
		wantErr string
	}{
		{name: "finally uses the shared group", setup: func(pr *tektonv1.PipelineRun) {
			task := pr.Spec.PipelineSpec.Tasks[0].DeepCopy()
			task.Name = "cleanup"
			pr.Spec.PipelineSpec.Finally = []tektonv1.PipelineTask{*task}
		}},
		{name: "missing group", setup: func(pr *tektonv1.PipelineRun) { pr.Spec.TaskRunTemplate.PodTemplate.SecurityContext.FSGroup = nil }, wantErr: "fsGroup"},
		{name: "per Task conflicting group", setup: func(pr *tektonv1.PipelineRun) {
			task := pr.Spec.PipelineSpec.Tasks[0].DeepCopy()
			task.Name = "other"
			pr.Spec.PipelineSpec.Tasks = append(pr.Spec.PipelineSpec.Tasks, *task)
			pr.Spec.TaskRunSpecs = []tektonv1.PipelineTaskRunSpec{{PipelineTaskName: "other", PodTemplate: &tektonpod.PodTemplate{SecurityContext: &corev1.PodSecurityContext{FSGroup: ptr.To(int64(1234))}}}}
		}, wantErr: "must share fsGroup"},
		{name: "unsupported source placement", setup: func(pr *tektonv1.PipelineRun) { pr.Spec.TaskRunTemplate.PodTemplate.Affinity = &corev1.Affinity{} }, wantErr: "default scheduling"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := &tektonv1.PipelineRun{}
			assert.NilError(t, yaml.Unmarshal([]byte(testPipeline), pr))
			tt.setup(pr)
			_, err := admittedContract(pr, "source")
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}
