package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

const testConfig = `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: http://localhost:8080
contexts:
- name: current
  context:
    cluster: test
    namespace: dev
- name: other
  context:
    cluster: test
    namespace: other-ns
current-context: current
`

func TestKubeconfigDefaultsAndOverrides(t *testing.T) {
	tests := []struct {
		name      string
		context   string
		namespace string
		unsetNS   bool
		wantCtx   string
		wantNS    string
	}{
		{name: "current context and namespace", wantCtx: "current", wantNS: "dev"},
		{name: "default namespace", unsetNS: true, wantCtx: "current", wantNS: "default"},
		{name: "namespace override", namespace: "override", wantCtx: "current", wantNS: "override"},
		{name: "context override", context: "other", wantCtx: "other", wantNS: "other-ns"},
		{name: "both overrides", context: "other", namespace: "override", wantCtx: "other", wantNS: "override"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := testConfig
			if tt.unsetNS {
				config = strings.Replace(config, "    namespace: dev\n", "", 1)
			}
			filename := filepath.Join(t.TempDir(), "config")
			assert.NilError(t, os.WriteFile(filename, []byte(config), 0o600))
			options := &Options{Kubeconfig: filename, Context: tt.context, Namespace: tt.namespace}
			clients, err := New(options)
			assert.NilError(t, err)
			assert.Equal(t, tt.wantCtx, options.Context)
			assert.Equal(t, tt.wantNS, options.Namespace)
			assert.Equal(t, "http://localhost:8080", clients.Config.Host)
		})
	}
}
