package localexecution

import (
	"testing"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	"gotest.tools/v3/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestCoschedulingNamespace(t *testing.T) {
	tests := []struct {
		name       string
		openShift  bool
		namespace  string
		installed  string
		coschedule string
		singleNode bool
		wantErr    string
	}{
		{name: "Kubernetes default", installed: "tekton-pipelines", coschedule: "workspaces"},
		{name: "OpenShift default", openShift: true, installed: "openshift-pipelines", coschedule: "workspaces"},
		{name: "explicit custom namespace", openShift: true, namespace: "custom-tekton", installed: "custom-tekton", coschedule: "pipelineruns"},
		{name: "explicit namespace does not fall back", openShift: true, namespace: "wrong-namespace", installed: "openshift-pipelines", coschedule: "workspaces", wantErr: "namespace wrong-namespace"},
		{name: "missing configuration", openShift: true, wantErr: "namespace openshift-pipelines (override with --tekton-namespace)"},
		{name: "scheduling still required", openShift: true, installed: "openshift-pipelines", coschedule: "disabled", wantErr: "requires Tekton coschedule"},
		{name: "single node needs no configuration", singleNode: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kube := kubefake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "first"}})
			if !tt.singleNode {
				_, err := kube.CoreV1().Nodes().Create(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "second"}}, metav1.CreateOptions{})
				assert.NilError(t, err)
			}
			if tt.installed != "" {
				_, err := kube.CoreV1().ConfigMaps(tt.installed).Create(t.Context(), &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "feature-flags"},
					Data:       map[string]string{"coschedule": tt.coschedule},
				}, metav1.CreateOptions{})
				assert.NilError(t, err)
			}
			err := checkCoscheduling(t.Context(), &cluster.Clients{Kube: kube}, tt.namespace, tt.openShift)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}
