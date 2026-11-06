package localexecution

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"

	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tektoncd/pipeline/pkg/substitution"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

type checkoutReplacement struct {
	task tektonv1.PipelineTask
}

func pipelineParams(pr *tektonv1.PipelineRun) map[string]tektonv1.ParamValue {
	params := map[string]tektonv1.ParamValue{}
	for _, p := range pr.Spec.PipelineSpec.Params {
		if p.Default != nil {
			params[p.Name] = *p.Default
		}
	}
	for _, p := range pr.Spec.Params {
		params[p.Name] = p.Value
	}
	return params
}

func suspectedCheckout(task tektonv1.PipelineTask) bool {
	if strings.Contains(task.Name, "git-clone") {
		return true
	}
	if task.TaskSpec == nil {
		return false
	}
	for _, step := range task.TaskSpec.Steps {
		command := step.Script + " " + strings.Join(step.Command, " ") + " " + strings.Join(step.Args, " ")
		if strings.Contains(command, "git clone") || strings.Contains(command, "git-init") || strings.Contains(step.Image, "git-init") {
			return true
		}
	}
	return false
}

func replaceCheckout(pr *tektonv1.PipelineRun, source *Snapshot, workspace, override string, known map[string]bool) (*checkoutReplacement, error) {
	params := pipelineParams(pr)
	selected := -1
	found := override == ""
	for _, task := range pr.Spec.PipelineSpec.Finally {
		if override != "" && task.Name == override {
			return nil, fmt.Errorf("--checkout-task %s is in finally; source checkout must be a main Pipeline task", override)
		}
		if sourceConsumer(task, workspace) && (task.Name == "git-clone" || known[task.Name] || suspectedCheckout(task)) && !definitelySkipped(task, params) {
			return nil, fmt.Errorf("checkout task %q is in finally; source checkout must be a main Pipeline task", task.Name)
		}
	}
	for i, task := range pr.Spec.PipelineSpec.Tasks {
		explicit := override != "" && task.Name == override
		found = found || explicit
		if !explicit && (override != "" || !sourceConsumer(task, workspace) || (task.Name != "git-clone" && !known[task.Name])) {
			continue
		}
		if definitelySkipped(task, params) {
			continue
		}
		if selected != -1 {
			return nil, fmt.Errorf("multiple source checkout tasks; select one with --checkout-task NAME and disable other checkouts of the source workspace")
		}
		if !explicit && (task.TaskSpec == nil || len(task.TaskSpec.Steps) != 1 || task.TaskSpec.Steps[0].Name != "clone" || len(task.TaskSpec.Sidecars) != 0) {
			return nil, fmt.Errorf("task %q is not a dedicated single-step checkout; use --checkout-task %s only if the entire task can be replaced", task.Name, task.Name)
		}
		selected = i
	}
	if !found {
		return nil, fmt.Errorf("--checkout-task %q does not name a main Pipeline task", override)
	}
	if selected == -1 {
		return nil, nil
	}
	task := &pr.Spec.PipelineSpec.Tasks[selected]
	if task.TaskRef != nil || task.TaskSpec == nil || task.TaskSpec.APIVersion != "" || task.TaskSpec.Kind != "" || len(task.TaskSpec.Spec.Raw) != 0 {
		return nil, fmt.Errorf("checkout task %q must resolve to an ordinary inline Task", task.Name)
	}
	if task.Matrix != nil {
		return nil, fmt.Errorf("checkout task %q uses an unsupported matrix", task.Name)
	}
	for _, step := range task.TaskSpec.Steps {
		if step.Ref != nil {
			return nil, fmt.Errorf("checkout task %q uses an unsupported StepAction reference", task.Name)
		}
	}
	var bindings []tektonv1.WorkspacePipelineTaskBinding
	for _, binding := range task.Workspaces {
		if binding.Workspace == workspace {
			bindings = append(bindings, binding)
		}
	}
	if len(bindings) != 1 || bindings[0].SubPath != "" {
		return nil, fmt.Errorf("checkout task %q must bind exactly one source workspace at its root", task.Name)
	}
	var declarations []tektonv1.WorkspaceDeclaration
	for _, declaration := range task.TaskSpec.Workspaces {
		if declaration.Name == bindings[0].Name {
			declarations = append(declarations, declaration)
		}
	}
	if len(declarations) != 1 {
		return nil, fmt.Errorf("checkout task %q must declare source workspace %q", task.Name, bindings[0].Name)
	}
	for _, spec := range task.TaskSpec.Params {
		if spec.Name != "subdirectory" {
			continue
		}
		value := spec.Default
		for _, p := range task.Params {
			if p.Name == spec.Name {
				value = &p.Value
			}
		}
		replacements := map[string]string{}
		for name, p := range params {
			if p.Type == "" || p.Type == tektonv1.ParamTypeString {
				replacements["params."+name] = p.StringVal
			}
		}
		if value == nil || (value.Type != "" && value.Type != tektonv1.ParamTypeString) || substitution.ApplyReplacements(value.StringVal, replacements) != "" {
			return nil, fmt.Errorf("checkout task %q uses an unsupported source subdirectory", task.Name)
		}
	}
	results, err := checkoutResultReferences(pr, task.Name)
	if err != nil {
		return nil, err
	}
	step := tektonv1.Step{
		Name: "local-source", Image: uploaderImage,
		Script:          "#!/bin/sh\nset -eu\ntest -d \"$LOCAL_SOURCE\"\n",
		Env:             []corev1.EnvVar{{Name: "LOCAL_SOURCE", Value: "$(workspaces." + bindings[0].Name + ".path)"}},
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
	}
	var produced []tektonv1.TaskResult
	for _, result := range task.TaskSpec.Results {
		value := ""
		supported := result.Type == "" || result.Type == tektonv1.ResultsTypeString
		switch result.Name {
		case "commit":
			value = source.Head
		case "committer-date":
			value = source.CommitterDate
		case "url":
			for _, spec := range task.TaskSpec.Params {
				if spec.Name == "url" && (spec.Type == "" || spec.Type == tektonv1.ParamTypeString) {
					value = "$(params.url)"
				}
			}
		default:
			supported = false
		}
		if !supported || value == "" {
			if results[result.Name] {
				return nil, fmt.Errorf("checkout task %q has consumed result %q that cannot be supplied from local source", task.Name, result.Name)
			}
			continue
		}
		delete(results, result.Name)
		result.Value = nil
		produced = append(produced, result)
		index := fmt.Sprint(len(produced))
		step.Env = append(step.Env, corev1.EnvVar{Name: "LOCAL_RESULT_" + index, Value: value}, corev1.EnvVar{Name: "RESULT_PATH_" + index, Value: "$(results." + result.Name + ".path)"})
		step.Script += "printf '%s' \"$LOCAL_RESULT_" + index + "\" > \"$RESULT_PATH_" + index + "\"\n"
	}
	if len(results) > 0 {
		return nil, fmt.Errorf("checkout task %q has unsupported consumed results: %s", task.Name, strings.Join(slices.Sorted(maps.Keys(results)), ", "))
	}
	step.Script += "printf '%s\\n' 'Using uploaded local source'\n"
	task.TaskSpec = &tektonv1.EmbeddedTask{
		Metadata: task.TaskSpec.Metadata,
		TaskSpec: tektonv1.TaskSpec{Params: task.TaskSpec.Params, Workspaces: declarations, Results: produced, Steps: []tektonv1.Step{step}},
	}
	task.Workspaces = bindings
	return &checkoutReplacement{task: *task.DeepCopy()}, nil
}

func checkoutResultReferences(pr *tektonv1.PipelineRun, name string) (map[string]bool, error) {
	copy := pr.DeepCopy()
	for i := range copy.Spec.PipelineSpec.Tasks {
		if copy.Spec.PipelineSpec.Tasks[i].Name == name {
			copy.Spec.PipelineSpec.Tasks[i].TaskSpec = nil
		}
	}
	data, err := json.Marshal(copy.Spec)
	if err != nil {
		return nil, err
	}
	var document any
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	results := map[string]bool{}
	var visit func(any) error
	visit = func(value any) error {
		switch value := value.(type) {
		case string:
			names, _, invalid := substitution.ExtractVariablesFromString(value, "tasks\\."+regexp.QuoteMeta(name)+"\\.results")
			if invalid != "" {
				return fmt.Errorf("checkout task %q: %s", name, invalid)
			}
			for _, name := range names {
				results[name] = true
			}
		case []any:
			for _, item := range value {
				if err := visit(item); err != nil {
					return err
				}
			}
		case map[string]any:
			for _, item := range value {
				if err := visit(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return results, visit(document)
}

func (r *checkoutReplacement) validate(pr *tektonv1.PipelineRun) error {
	if r == nil {
		return nil
	}
	for _, task := range pr.Spec.PipelineSpec.Tasks {
		if task.Name != r.task.Name {
			continue
		}
		expected, actual := r.task.DeepCopy(), task.DeepCopy()
		expected.SetDefaults(context.Background())
		actual.SetDefaults(context.Background())
		if !reflect.DeepEqual(expected, actual) {
			return fmt.Errorf("local source replacement task %q changed during admission", task.Name)
		}
		return nil
	}
	return fmt.Errorf("local source replacement task %q disappeared during admission", r.task.Name)
}
