package localexecution

import (
	"context"
	"fmt"
	"io"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/utils/ptr"
)

const uploaderImage = "docker.io/library/busybox:1.37.0@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec"

func uploaderPod(name, namespace, pvc string, contract *executionContract) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:        ptr.To(int64(300)),
			AutomountServiceAccountToken: ptr.To(false),
			NodeSelector:                 contract.NodeSelector,
			Tolerations:                  contract.Tolerations,
			SecurityContext:              &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(contract.Group), RunAsGroup: ptr.To(contract.Group), FSGroup: ptr.To(contract.Group), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{
				Name: "upload", Image: uploaderImage, Command: []string{"sleep", "300"},
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("128Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("16Mi")},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "source", MountPath: "/source"}},
			}},
			Volumes: []corev1.Volume{{Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc}}}},
		},
	}
}

func (e *Execution) upload(ctx context.Context, pod *corev1.Pod, archive io.Reader) error {
	request := e.clients.Kube.CoreV1().RESTClient().Post().Namespace(pod.Namespace).Resource("pods").Name(pod.Name).SubResource("exec").VersionedParams(&corev1.PodExecOptions{
		Container: "upload", Command: []string{"tar", "-xmpf", "-", "-C", "/source", "--no-same-owner"}, Stdin: true, Stdout: true, Stderr: true, TTY: false,
	}, scheme.ParameterCodec)
	websocket, err := remotecommand.NewWebSocketExecutor(e.config, http.MethodGet, request.URL().String())
	if err != nil {
		return err
	}
	spdy, err := remotecommand.NewSPDYExecutor(e.config, http.MethodPost, request.URL())
	if err != nil {
		return err
	}
	executor, err := remotecommand.NewFallbackExecutor(websocket, spdy, func(err error) bool { return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err) })
	if err != nil {
		return err
	}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: archive, Stdout: e.streams.ErrOut, Stderr: e.streams.ErrOut}); err != nil {
		return fmt.Errorf("source upload/extraction failed: %w", err)
	}
	return nil
}
