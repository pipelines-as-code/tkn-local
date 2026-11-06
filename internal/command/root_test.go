package command

import (
	"bytes"
	"strings"
	"testing"

	"github.com/pipelines-as-code/tkn-local/internal/iostreams"
	"gotest.tools/v3/assert"
)

func TestRunInputValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "missing pipeline", args: []string{"run"}, wantErr: "--pipeline is required"},
		{name: "context and namespace optional", args: []string{"run", "--pipeline", "p.yaml"}, wantErr: "requires --yes"},
		{name: "explicit target overrides accepted", args: []string{"run", "--pipeline", "p.yaml", "--context", "dev-context", "-n", "dev"}, wantErr: "requires --yes"},
		{name: "noninteractive acknowledgement required", args: []string{"run", "--pipeline", "p.yaml", "-n", "dev"}, wantErr: "requires --yes"},
		{name: "invalid size", args: []string{"run", "--pipeline", "p.yaml", "-n", "dev", "--storage-size", "bad"}, wantErr: "storage quantity"},
		{name: "invalid timeout", args: []string{"run", "--pipeline", "p.yaml", "-n", "dev", "--timeout", "0"}, wantErr: "must be positive"},
		{name: "malformed parameter", args: []string{"run", "--pipeline", "p.yaml", "-n", "dev", "-p", "bad"}, wantErr: "expected key=value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			streams := &iostreams.IOStreams{In: strings.NewReader(""), Out: out, ErrOut: out}
			root := New(streams)
			root.SilenceErrors = true
			root.SetArgs(tt.args)
			assert.ErrorContains(t, root.Execute(), tt.wantErr)
			assert.Equal(t, 0, out.Len())
		})
	}
}

func TestParseParams(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		want    map[string]string
		wantErr string
	}{
		{name: "first equals and empty", input: []string{"value=a=b", "empty="}, want: map[string]string{"value": "a=b", "empty": ""}},
		{name: "last assignment wins", input: []string{"key=one", "key=two"}, want: map[string]string{"key": "two"}},
		{name: "missing equals", input: []string{"key"}, wantErr: "expected key=value"},
		{name: "empty key", input: []string{"=value"}, wantErr: "expected key=value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, err := parseParams(tt.input)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NilError(t, err)
			assert.DeepEqual(t, tt.want, values)
		})
	}
}
