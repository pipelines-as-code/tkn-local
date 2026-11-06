package localexecution

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	tektonpod "github.com/tektoncd/pipeline/pkg/apis/pipeline/pod"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
)

func pipelineTasks(pr *tektonv1.PipelineRun) []tektonv1.PipelineTask {
	return append(slices.Clone(pr.Spec.PipelineSpec.Tasks), pr.Spec.PipelineSpec.Finally...)
}

func validatePipeline(pr *tektonv1.PipelineRun, source string) (string, error) {
	if source == "" {
		return "", fmt.Errorf("--workspace must name the Pipeline workspace that receives local source")
	}
	if pr.Spec.PipelineRef != nil || pr.Spec.PipelineSpec == nil {
		return "", fmt.Errorf("local execution requires a resolved inline pipelineSpec; resolver references are unsupported")
	}
	declared := false
	bound := map[string]bool{}
	for _, w := range pr.Spec.Workspaces {
		if bound[w.Name] {
			return "", fmt.Errorf("duplicate workspace binding %q", w.Name)
		}
		bound[w.Name] = true
		if w.Name == source {
			if w.SubPath != "" || w.PersistentVolumeClaim != nil || w.Secret != nil || w.ConfigMap != nil || w.CSI != nil || w.Projected != nil {
				return "", fmt.Errorf("source workspace %q cannot have a subPath or an existing PVC, Secret, ConfigMap, CSI or projected binding", source)
			}
		}
	}
	for _, w := range pr.Spec.PipelineSpec.Workspaces {
		if w.Name == source {
			declared = true
		}
		if w.Name != source && !w.Optional && !bound[w.Name] {
			return "", fmt.Errorf("required workspace %q is unbound", w.Name)
		}
	}
	if !declared {
		return "", fmt.Errorf("source workspace %q is not declared by the Pipeline", source)
	}
	params := map[string]tektonv1.ParamValue{}
	for _, p := range pr.Spec.PipelineSpec.Params {
		if p.Default != nil {
			params[p.Name] = *p.Default
		}
	}
	for _, p := range pr.Spec.Params {
		params[p.Name] = p.Value
	}
	for _, p := range pr.Spec.PipelineSpec.Params {
		if _, ok := params[p.Name]; !ok {
			return "", fmt.Errorf("required Pipeline parameter %q is missing", p.Name)
		}
	}
	consumers := 0
	data, err := json.Marshal(pr.Spec.PipelineSpec)
	if err != nil {
		return "", err
	}
	for _, task := range pipelineTasks(pr) {
		if task.TaskRef != nil || task.TaskSpec == nil || task.TaskSpec.APIVersion != "" || task.TaskSpec.Kind != "" || len(task.TaskSpec.Spec.Raw) > 0 {
			return "", fmt.Errorf("task %q must resolve to an ordinary inline Task; Custom Tasks, ClusterTasks and resolver references are unsupported", task.Name)
		}
		ts := &task.TaskSpec.TaskSpec
		if err := validateTaskSecurity(ts); err != nil {
			return "", fmt.Errorf("task %q: %w", task.Name, err)
		}
		taskParams := map[string]bool{}
		for _, p := range task.Params {
			taskParams[p.Name] = true
		}
		for _, p := range ts.Params {
			if p.Default == nil && !taskParams[p.Name] {
				return "", fmt.Errorf("task %q requires parameter %q", task.Name, p.Name)
			}
		}
		taskBindings := map[string]bool{}
		for _, w := range task.Workspaces {
			taskBindings[w.Name] = true
			if w.Workspace == source {
				if w.SubPath != "" {
					return "", fmt.Errorf("task %q uses an unsupported source subPath", task.Name)
				}
				found := false
				for _, d := range ts.Workspaces {
					if d.Name == w.Name {
						found = true
					}
				}
				if !found {
					return "", fmt.Errorf("task %q maps source to undeclared workspace %q", task.Name, w.Name)
				}
				consumers++
			}
		}
		for _, w := range ts.Workspaces {
			if !w.Optional && !taskBindings[w.Name] {
				return "", fmt.Errorf("task %q requires workspace mapping %q", task.Name, w.Name)
			}
		}
		checkout := strings.Contains(task.Name, "git-clone")
		for _, step := range ts.Steps {
			if step.Ref != nil {
				return "", fmt.Errorf("task %q uses a StepAction reference, which is unsupported", task.Name)
			}
			script := step.Script + " " + strings.Join(step.Command, " ")
			checkout = checkout || strings.Contains(script, "git clone") || strings.Contains(script, "git-init") || strings.Contains(step.Image, "git-init")
		}
		if checkout {
			if !definitelySkipped(task, params) {
				return "", fmt.Errorf("task %q performs checkout; use a dedicated local PipelineRun or an author-owned when gate that disables it", task.Name)
			}
			if strings.Contains(string(data), "$(tasks."+task.Name+".results.") || strings.Contains(string(data), "$(finally."+task.Name+".results.") {
				return "", fmt.Errorf("task %q is a skipped checkout with consumed results; provide an author-owned local alternative", task.Name)
			}
		}
	}
	if consumers == 0 {
		return "", fmt.Errorf("no Task consumes source workspace %q", source)
	}
	return source, nil
}

func definitelySkipped(task tektonv1.PipelineTask, params map[string]tektonv1.ParamValue) bool {
	replace := func(s string) string {
		for name, value := range params {
			if value.Type == tektonv1.ParamTypeString || value.Type == "" {
				s = strings.ReplaceAll(s, "$(params."+name+")", value.StringVal)
			}
		}
		return s
	}
	for _, when := range task.When {
		input := replace(when.Input)
		if when.CEL != "" || strings.Contains(input, "$(") {
			continue
		}
		values := make([]string, 0, len(when.Values))
		known := true
		for _, v := range when.Values {
			v = replace(v)
			if strings.Contains(v, "$(") {
				known = false
			}
			values = append(values, v)
		}
		if known && ((string(when.Operator) == "in" && !slices.Contains(values, input)) || (string(when.Operator) == "notin" && slices.Contains(values, input))) {
			return true
		}
	}
	return false
}

func validateTaskSecurity(ts *tektonv1.TaskSpec) error {
	for _, v := range ts.Volumes {
		if v.HostPath != nil {
			return fmt.Errorf("hostPath volumes are unsupported")
		}
	}
	if ts.StepTemplate != nil {
		if err := validateContainerSecurity(ts.StepTemplate.SecurityContext); err != nil {
			return err
		}
	}
	for _, s := range ts.Steps {
		if err := validateContainerSecurity(s.SecurityContext); err != nil {
			return err
		}
	}
	for _, s := range ts.Sidecars {
		if err := validateContainerSecurity(s.SecurityContext); err != nil {
			return err
		}
	}
	return nil
}

func validateContainerSecurity(sc *corev1.SecurityContext) error {
	if sc != nil && ((sc.Privileged != nil && *sc.Privileged) || (sc.AllowPrivilegeEscalation != nil && *sc.AllowPrivilegeEscalation) || (sc.Capabilities != nil && len(sc.Capabilities.Add) > 0) || (sc.RunAsUser != nil && *sc.RunAsUser == 0) || (sc.ProcMount != nil && *sc.ProcMount != corev1.DefaultProcMount)) {
		return fmt.Errorf("privileged, root or host-access containers are unsupported for local execution")
	}
	return nil
}

type executionIdentity struct {
	Task, ServiceAccount string
	PodTemplate          *tektonpod.PodTemplate
}

type executionContract struct {
	OpenShift    bool
	Group        int64
	NodeSelector map[string]string
	Tolerations  []corev1.Toleration
	Identities   []executionIdentity
}

func admittedContract(pr *tektonv1.PipelineRun, source string) (*executionContract, error) {
	contract := &executionContract{}
	haveGroup := false
	for _, task := range pipelineTasks(pr) {
		spec := pr.DeepCopy().GetTaskRunSpec(task.Name)
		template := spec.PodTemplate
		if template == nil {
			template = &tektonpod.PodTemplate{}
		}
		if template.HostNetwork {
			return nil, fmt.Errorf("task %q: host networking is unsupported", task.Name)
		}
		for _, v := range template.Volumes {
			if v.HostPath != nil {
				return nil, fmt.Errorf("task %q: hostPath volumes are unsupported", task.Name)
			}
		}
		if sc := template.SecurityContext; sc != nil && sc.RunAsUser != nil && *sc.RunAsUser == 0 {
			return nil, fmt.Errorf("task %q: root UID is unsupported", task.Name)
		}
		serviceAccount := spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = "default"
		}
		contract.Identities = append(contract.Identities, executionIdentity{Task: task.Name, ServiceAccount: serviceAccount, PodTemplate: template})
		consumes := false
		for _, w := range task.Workspaces {
			if w.Workspace == source {
				consumes = true
			}
		}
		if !consumes {
			continue
		}
		if template.Affinity != nil || len(template.TopologySpreadConstraints) > 0 || (template.SchedulerName != "" && template.SchedulerName != "default-scheduler") || template.RuntimeClassName != nil {
			return nil, fmt.Errorf("task %q: source requires default scheduling without affinity, topology spread or RuntimeClass", task.Name)
		}
		if template.SecurityContext == nil || template.SecurityContext.FSGroup == nil || *template.SecurityContext.FSGroup <= 0 {
			return nil, fmt.Errorf("task %q must declare a non-root securityContext.fsGroup for the source volume", task.Name)
		}
		group := *template.SecurityContext.FSGroup
		if !haveGroup {
			contract.Group = group
			contract.NodeSelector = template.NodeSelector
			contract.Tolerations = template.Tolerations
			haveGroup = true
		} else if group != contract.Group || !reflect.DeepEqual(template.NodeSelector, contract.NodeSelector) || !reflect.DeepEqual(template.Tolerations, contract.Tolerations) {
			return nil, fmt.Errorf("all source-consuming Tasks must share fsGroup, nodeSelector and tolerations")
		}
	}
	if !haveGroup {
		return nil, fmt.Errorf("no source-consuming Task")
	}
	return contract, nil
}
