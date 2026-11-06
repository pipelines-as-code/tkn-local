package localexecution

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

const testPipeline = `apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: local-test
spec:
  taskRunTemplate:
    podTemplate:
      securityContext:
        fsGroup: 65532
  pipelineSpec:
    workspaces:
      - name: source
    tasks:
      - name: test
        workspaces:
          - name: source
            workspace: source
        taskSpec:
          workspaces:
            - name: source
          steps:
            - name: read
              image: busybox
              script: cat $(workspaces.source.path)/tracked
`

func TestResolve(t *testing.T) {
	fixture, err := os.ReadFile("testdata/local.yaml")
	assert.NilError(t, err)
	tests := []struct {
		name     string
		pipeline string
		sibling  string
		params   map[string]string
		wantErr  string
	}{
		{name: "inline", pipeline: testPipeline},
		{name: "v1beta1 conversion", pipeline: strings.Replace(strings.Replace(testPipeline, "tekton.dev/v1", "tekton.dev/v1beta1", 1), "  taskRunTemplate:\n    podTemplate:\n      securityContext:\n        fsGroup: 65532", "  podTemplate:\n    securityContext:\n      fsGroup: 65532", 1)},
		{name: "e2e fixture", pipeline: string(fixture), params: map[string]string{"expected-tracked": strings.Repeat("a", 64), "expected-untracked": strings.Repeat("b", 64), "force-failure": "false"}},
		{name: "comment-only documents", pipeline: "# copyright\n---\n" + testPipeline + "\n---\n# empty document\n"},
		{name: "sibling PipelineRun ignored", pipeline: testPipeline, sibling: strings.ReplaceAll(testPipeline, "local-test", "other")},
		{name: "literal template syntax preserved", pipeline: strings.ReplaceAll(testPipeline, "image: busybox", "image: busybox\n              args: ['{{ literal }}']")},
		{name: "ambiguous selected document", pipeline: testPipeline + "\n---\n" + testPipeline, wantErr: "exactly one"},
		{name: "conflicting namespace", pipeline: strings.Replace(testPipeline, "  name: local-test", "  name: local-test\n  namespace: elsewhere", 1), wantErr: "conflicts"},
		{name: "controller state", pipeline: strings.Replace(testPipeline, "  name: local-test", "  name: local-test\n  uid: server-owned", 1), wantErr: "controller-owned"},
		{name: "source subpath", pipeline: strings.Replace(testPipeline, "spec:\n", "spec:\n  workspaces:\n    - name: source\n      subPath: sub\n      emptyDir: {}\n", 1), wantErr: "subPath"},
		{name: "existing PVC", pipeline: strings.Replace(testPipeline, "spec:\n", "spec:\n  workspaces:\n    - name: source\n      persistentVolumeClaim:\n        claimName: existing\n", 1), wantErr: "existing PVC"},
		{name: "checkout rejected", pipeline: strings.ReplaceAll(testPipeline, "script: cat $(workspaces.source.path)/tracked", "script: git clone https://example.com/repo ."), wantErr: "performs checkout"},
		{name: "unsupported documents", pipeline: testPipeline + "\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: credential\n", wantErr: "unsupported kind"},
		{name: "malformed selected YAML", pipeline: "apiVersion: [", wantErr: "document 1"},
		{name: "unknown fields", pipeline: strings.Replace(testPipeline, "spec:\n", "spec:\n  typo: true\n", 1), wantErr: "unknown field"},
		{name: "unresolved Task", pipeline: strings.Split(testPipeline, "        taskSpec:")[0] + "        taskRef:\n          name: missing\n", wantErr: "taskRef"},
		{name: "duplicate Task", pipeline: testPipeline, sibling: testTask + "\n---\n" + testTask, wantErr: "duplicate Task"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := testRepository(t)
			writeSource(t, root, ".tekton/local.yaml", tt.pipeline, 0o600)
			if tt.sibling != "" {
				writeSource(t, root, ".tekton/other.yaml", tt.sibling, 0o600)
			}
			snapshot, err := Capture(t.Context(), root, 1<<30)
			assert.NilError(t, err)
			defer func() { assert.NilError(t, snapshot.Close()) }()
			prepared, err := Resolve(t.Context(), snapshot, filepath.Join(root, ".tekton/local.yaml"), "dev", "source", tt.params)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NilError(t, err)
			assert.Equal(t, "source", prepared.Workspace)
			assert.Equal(t, "dev", prepared.PipelineRun.Namespace)
			assert.Assert(t, len(prepared.Definitions) > 0)
			assert.NilError(t, applySecurityDefaults(prepared.PipelineRun, prepared.Workspace, securityDefaults{Group: defaultUser}))
			contract, err := admittedContract(prepared.PipelineRun, prepared.Workspace)
			assert.NilError(t, err)
			assert.Equal(t, int64(65532), contract.Group)
			original, err := os.ReadFile(filepath.Join(root, ".tekton/local.yaml"))
			assert.NilError(t, err)
			assert.Equal(t, tt.pipeline, string(original))
		})
	}
}

const testTask = `apiVersion: tekton.dev/v1
kind: Task
metadata:
  name: read
spec:
  workspaces:
    - name: source
  steps:
    - name: read
      image: busybox
      script: echo captured
`

func TestResolveCapturedReferences(t *testing.T) {
	root := testRepository(t)
	pipeline := `apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: local-test
spec:
  taskRunTemplate:
    podTemplate:
      securityContext:
        fsGroup: 65532
  pipelineRef:
    name: build
---
apiVersion: tekton.dev/v1
kind: Pipeline
metadata:
  name: build
spec:
  params:
    - name: message
      type: string
      default: original
  workspaces:
    - name: source
  tasks:
    - name: read
      workspaces:
        - name: source
          workspace: source
      taskRef:
        name: read
  finally:
    - name: cleanup
      workspaces:
        - name: source
          workspace: source
      taskRef:
        name: read
`
	writeSource(t, root, ".tekton/local.yaml", pipeline, 0o600)
	writeSource(t, root, ".tekton/read.yaml", testTask, 0o600)
	snapshot, err := Capture(t.Context(), root, 1<<30)
	assert.NilError(t, err)
	defer func() { assert.NilError(t, snapshot.Close()) }()
	writeSource(t, root, ".tekton/read.yaml", "not the snapshot", 0o600)
	prepared, err := Resolve(t.Context(), snapshot, filepath.Join(root, ".tekton/local.yaml"), "dev", "source", map[string]string{"message": "override"})
	assert.NilError(t, err)
	assert.Equal(t, "echo captured", prepared.PipelineRun.Spec.PipelineSpec.Tasks[0].TaskSpec.Steps[0].Script)
	assert.Equal(t, "echo captured", prepared.PipelineRun.Spec.PipelineSpec.Finally[0].TaskSpec.Steps[0].Script)
	assert.Equal(t, "override", prepared.PipelineRun.Spec.Params[0].Value.StringVal)
}
