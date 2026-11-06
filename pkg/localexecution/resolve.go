package localexecution

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	tektonv1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	kyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const RunIDLabel = "local.tekton.dev/run-id"

type Definition struct {
	Origin string
	Digest string
}

type Prepared struct {
	Digest      string
	PipelineRun *tektonv1.PipelineRun
	Workspace   string
	Definitions []Definition
}

func Resolve(ctx context.Context, source *Snapshot, filename, namespace, workspace string, inputs map[string]string) (*Prepared, error) {
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}
	selected, err := filepath.Rel(source.Root, absolute)
	if err != nil {
		return nil, err
	}
	selected = filepath.ToSlash(selected)
	if !validPath(selected) {
		return nil, fmt.Errorf("--file must be inside --source")
	}
	filenames := []string{selected}
	for _, entry := range source.Files {
		if entry.Path != selected && strings.HasPrefix(entry.Path, ".tekton/") && (strings.HasSuffix(entry.Path, ".yaml") || strings.HasSuffix(entry.Path, ".yml")) {
			filenames = append(filenames, entry.Path)
		}
	}
	scheme := runtime.NewScheme()
	if err := tektonv1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := tektonv1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	decoder := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()
	var runs []*tektonv1.PipelineRun
	pipelines := map[string]*tektonv1.Pipeline{}
	tasks := map[string]*tektonv1.Task{}
	prepared := &Prepared{}
	for _, name := range filenames {
		data, err := source.ReadFile(name)
		if err != nil {
			return nil, err
		}
		reader := kyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
		for document := 1; ; document++ {
			raw, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("%s document %d: %w", name, document, err)
			}
			obj, err := decodeDefinition(ctx, decoder, raw, name != selected)
			if err != nil {
				return nil, fmt.Errorf("%s document %d: %w", name, document, err)
			}
			if obj == nil {
				continue
			}
			switch obj := obj.(type) {
			case *tektonv1.PipelineRun:
				runs = append(runs, obj)
			case *tektonv1.Pipeline:
				if obj.Name == "" || pipelines[obj.Name] != nil {
					return nil, fmt.Errorf("missing or duplicate Pipeline name %q", obj.Name)
				}
				pipelines[obj.Name] = obj
			case *tektonv1.Task:
				if obj.Name == "" || tasks[obj.Name] != nil {
					return nil, fmt.Errorf("missing or duplicate Task name %q", obj.Name)
				}
				tasks[obj.Name] = obj
			}
			digest := sha256.Sum256(raw)
			prepared.Definitions = append(prepared.Definitions, Definition{Origin: fmt.Sprintf("%s document %d", name, document), Digest: hex.EncodeToString(digest[:])})
		}
	}
	if len(runs) != 1 {
		return nil, fmt.Errorf("%s must contain exactly one PipelineRun (found %d)", selected, len(runs))
	}
	pr := runs[0]
	if err := validateIdentity(pr, namespace); err != nil {
		return nil, err
	}
	if pr.Spec.PipelineRef != nil {
		ref := pr.Spec.PipelineRef
		if pr.Spec.PipelineSpec != nil || ref.Resolver != "" || pipelines[ref.Name] == nil {
			return nil, fmt.Errorf("pipelineRef %q must name a Pipeline in the selected file or .tekton", ref.Name)
		}
		pr.Spec.PipelineSpec = pipelines[ref.Name].Spec.DeepCopy()
		pr.Spec.PipelineRef = nil
	}
	if pr.Spec.PipelineSpec == nil {
		return nil, fmt.Errorf("PipelineRun must declare pipelineSpec or pipelineRef")
	}
	for _, group := range [][]tektonv1.PipelineTask{pr.Spec.PipelineSpec.Tasks, pr.Spec.PipelineSpec.Finally} {
		for i := range group {
			task := &group[i]
			if task.TaskRef == nil {
				continue
			}
			ref := task.TaskRef
			if task.TaskSpec != nil || ref.Resolver != "" || (ref.Kind != "" && ref.Kind != "Task") || (ref.APIVersion != "" && ref.APIVersion != "tekton.dev/v1" && ref.APIVersion != "tekton.dev/v1beta1") || tasks[ref.Name] == nil {
				return nil, fmt.Errorf("taskRef %q must name an ordinary Task in the selected file or .tekton", ref.Name)
			}
			definition := tasks[ref.Name]
			task.TaskSpec = &tektonv1.EmbeddedTask{TaskSpec: *definition.Spec.DeepCopy(), Metadata: tektonv1.PipelineTaskMetadata{Labels: definition.Labels, Annotations: definition.Annotations}}
			task.TaskRef = nil
		}
	}
	for _, name := range slices.Sorted(maps.Keys(inputs)) {
		value := inputs[name]
		for _, spec := range pr.Spec.PipelineSpec.Params {
			if spec.Name == name && spec.Type != "" && spec.Type != tektonv1.ParamTypeString {
				return nil, fmt.Errorf("--param %s only supports string parameters", name)
			}
		}
		param := tektonv1.Param{Name: name, Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: value}}
		found := false
		for i := range pr.Spec.Params {
			if pr.Spec.Params[i].Name == name {
				pr.Spec.Params[i] = param
				found = true
			}
		}
		if !found {
			pr.Spec.Params = append(pr.Spec.Params, param)
		}
	}
	if _, err := validatePipeline(pr, workspace); err != nil {
		return nil, err
	}
	pr.Namespace = namespace
	spec, err := json.Marshal(pr.Spec)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(spec)
	prepared.Digest = hex.EncodeToString(digest[:])
	prepared.PipelineRun = pr
	prepared.Workspace = workspace
	return prepared, nil
}

//nolint:staticcheck // Accept v1beta1 manifests and convert them to v1.
func decodeDefinition(ctx context.Context, decoder runtime.Decoder, raw []byte, sibling bool) (runtime.Object, error) {
	data, err := yaml.YAMLToJSONStrict(raw)
	if err != nil {
		return nil, err
	}
	var header *metav1.TypeMeta
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, err
	}
	if header == nil || (header.Kind == "PipelineRun" && sibling) {
		return nil, nil
	}
	if header.Kind != "PipelineRun" && header.Kind != "Pipeline" && header.Kind != "Task" {
		return nil, fmt.Errorf("unsupported kind %q; only PipelineRuns, Pipelines and Tasks are read", header.Kind)
	}
	if header.APIVersion != "tekton.dev/v1" && header.APIVersion != "tekton.dev/v1beta1" {
		return nil, fmt.Errorf("unsupported apiVersion %q", header.APIVersion)
	}
	obj, _, err := decoder.Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	switch old := obj.(type) {
	case *tektonv1beta1.PipelineRun:
		converted := &tektonv1.PipelineRun{}
		return converted, old.ConvertTo(ctx, converted)
	case *tektonv1beta1.Pipeline:
		converted := &tektonv1.Pipeline{}
		return converted, old.ConvertTo(ctx, converted)
	case *tektonv1beta1.Task:
		converted := &tektonv1.Task{}
		return converted, old.ConvertTo(ctx, converted)
	}
	return obj, nil
}

func validateIdentity(pr *tektonv1.PipelineRun, namespace string) error {
	if pr.Namespace != "" && pr.Namespace != namespace {
		return fmt.Errorf("YAML namespace %q conflicts with selected namespace %q", pr.Namespace, namespace)
	}
	if pr.Spec.Status != "" || pr.UID != "" || pr.ResourceVersion != "" || !pr.CreationTimestamp.IsZero() || pr.Generation != 0 || len(pr.OwnerReferences) > 0 || len(pr.ManagedFields) > 0 || len(pr.Finalizers) > 0 || pr.DeletionTimestamp != nil || !reflect.DeepEqual(pr.Status, tektonv1.PipelineRunStatus{}) {
		return fmt.Errorf("input must be a declarative PipelineRun without status or controller-owned identity")
	}
	return nil
}
