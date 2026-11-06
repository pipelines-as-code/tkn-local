package localexecution

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"time"

	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/apis"
)

func (e *Execution) Run(ctx context.Context) (resultErr error) {
	var createdPR *tektonv1.PipelineRun
	var createdPVC *corev1.PersistentVolumeClaim
	var createdPod *corev1.Pod
	defer func() {
		if createdPR == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		cleanupErr := e.cleanup(cleanupCtx, createdPR, createdPVC, createdPod, resultErr != nil)
		if cleanupErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("cleanup: %w", cleanupErr))
			e.recovery()
		} else if e.options.KeepResources {
			e.recovery()
		}
	}()
	preparationCtx, cancelPreparation := context.WithTimeout(ctx, preparationTimeout)
	defer cancelPreparation()
	desired := e.pipelineRun.DeepCopy()
	clearIdentity(&desired.ObjectMeta)
	var err error
	createdPR, err = e.clients.Tekton.TektonV1().PipelineRuns(e.options.Namespace).Create(preparationCtx, desired, metav1.CreateOptions{})
	if err != nil {
		e.recovery()
		return fmt.Errorf("create pending PipelineRun: %w", err)
	}
	if createdPR.UID == "" {
		return fmt.Errorf("API server returned a PipelineRun without a UID")
	}
	fmt.Fprintf(e.streams.ErrOut, "Created pending PipelineRun %s/%s\n", createdPR.Namespace, createdPR.Name)
	if !reflect.DeepEqual(createdPR.Spec, e.pipelineRun.Spec) {
		return fmt.Errorf("admission changed the pending PipelineRun after preview; source was not uploaded")
	}
	if _, err := admittedContract(createdPR, e.prepared.Workspace); err != nil {
		return err
	}
	owner := *metav1.NewControllerRef(createdPR, tektonv1.SchemeGroupVersion.WithKind("PipelineRun"))
	pvc := e.pvc.DeepCopy()
	clearIdentity(&pvc.ObjectMeta)
	pvc.OwnerReferences = []metav1.OwnerReference{owner}
	createdPVC, err = e.clients.Kube.CoreV1().PersistentVolumeClaims(e.options.Namespace).Create(preparationCtx, pvc, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create fresh source PVC: %w", err)
	}
	if !reflect.DeepEqual(createdPVC.Spec, e.pvc.Spec) {
		return fmt.Errorf("admission changed the PVC after preview")
	}
	pod := e.pod.DeepCopy()
	clearIdentity(&pod.ObjectMeta)
	pod.OwnerReferences = []metav1.OwnerReference{owner}
	createdPod, err = e.clients.Kube.CoreV1().Pods(e.options.Namespace).Create(preparationCtx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create uploader Pod: %w", err)
	}
	if !reflect.DeepEqual(createdPod.Spec, e.pod.Spec) {
		return fmt.Errorf("admission changed uploader security or placement after preview; source was not uploaded")
	}
	if err := e.waitUploader(preparationCtx, createdPod, createdPVC); err != nil {
		return err
	}
	currentPod, err := e.clients.Kube.CoreV1().Pods(e.options.Namespace).Get(preparationCtx, createdPod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	expectedPod := e.pod.DeepCopy()
	currentPod.Spec.NodeName = ""
	expectedPod.Spec.NodeName = ""
	if currentPod.UID != createdPod.UID || !reflect.DeepEqual(currentPod.Spec, expectedPod.Spec) {
		return fmt.Errorf("uploader changed before upload")
	}
	archive, err := e.source.Archive()
	if err != nil {
		return err
	}
	fmt.Fprintf(e.streams.ErrOut, "Uploading source to PVC %s\n", createdPVC.Name)
	if err := e.uploadSource(preparationCtx, createdPod, archive); err != nil {
		return err
	}
	if err := e.deleteUploader(preparationCtx, createdPod); err != nil {
		return err
	}
	createdPod = nil
	current, err := e.clients.Tekton.TektonV1().PipelineRuns(e.options.Namespace).Get(preparationCtx, createdPR.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if current.UID != createdPR.UID || current.Spec.Status != tektonv1.PipelineRunSpecStatusPending || !reflect.DeepEqual(current.Spec, createdPR.Spec) {
		return fmt.Errorf("pending PipelineRun changed during upload; refusing to resume")
	}
	current.Spec.Status = ""
	if _, err := e.clients.Tekton.TektonV1().PipelineRuns(e.options.Namespace).Update(preparationCtx, current, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("resume uploaded PipelineRun: %w", err)
	}
	cancelPreparation()
	executionCtx, cancelExecution := context.WithTimeout(ctx, e.options.Timeout)
	defer cancelExecution()
	fmt.Fprintf(e.streams.ErrOut, "Running %s/%s\n", createdPR.Namespace, createdPR.Name)
	logCtx, cancelLogs := context.WithCancel(executionCtx)
	logDone := make(chan error, 1)
	go func() { logDone <- e.followLogs(logCtx, createdPR.Name) }()
	terminal, statusErr := e.waitPipelineRun(executionCtx, createdPR)
	var logErr error
	if statusErr != nil {
		cancelLogs()
		logErr = <-logDone
	} else {
		timer := time.NewTimer(30 * time.Second)
		select {
		case logErr = <-logDone:
		case <-timer.C:
			cancelLogs()
			logErr = errors.Join(fmt.Errorf("live logs did not drain within 30 seconds"), <-logDone)
		case <-ctx.Done():
			cancelLogs()
			logErr = errors.Join(ctx.Err(), <-logDone)
		}
		timer.Stop()
	}
	cancelLogs()
	if logErr != nil {
		logErr = fmt.Errorf("live logs: %w", logErr)
	}
	if terminal != nil {
		condition := terminal.Status.GetCondition(apis.ConditionSucceeded)
		if condition != nil {
			fmt.Fprintf(e.streams.ErrOut, "PipelineRun %s: %s (%s)\n", terminal.Name, condition.Status, condition.Reason)
			if condition.Status == corev1.ConditionFalse {
				statusErr = fmt.Errorf("PipelineRun %s failed: %s: %s", terminal.Name, condition.Reason, condition.Message)
			}
		}
	}
	return errors.Join(statusErr, logErr)
}

func clearIdentity(meta *metav1.ObjectMeta) {
	meta.UID = ""
	meta.ResourceVersion = ""
	meta.CreationTimestamp = metav1.Time{}
	meta.Generation = 0
	meta.ManagedFields = nil
}

func (e *Execution) waitUploader(ctx context.Context, pod *corev1.Pod, pvc *corev1.PersistentVolumeClaim) error {
	detail := "waiting for storage and uploader scheduling"
	err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		claim, err := e.clients.Kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if claim.UID != pvc.UID {
			return false, fmt.Errorf("source PVC was replaced")
		}
		if claim.Status.Phase == corev1.ClaimLost {
			return false, fmt.Errorf("source PVC is Lost")
		}
		p, err := e.clients.Kube.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if p.UID != pod.UID {
			return false, fmt.Errorf("uploader Pod was replaced")
		}
		if p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("uploader exited before upload: %s %s", p.Status.Reason, p.Status.Message)
		}
		for _, status := range p.Status.ContainerStatuses {
			if status.State.Waiting != nil {
				detail = status.State.Waiting.Reason + ": " + status.State.Waiting.Message
				if status.State.Waiting.Reason == "ImagePullBackOff" || status.State.Waiting.Reason == "CreateContainerConfigError" {
					return false, fmt.Errorf("uploader: %s", detail)
				}
			}
		}
		for _, condition := range p.Status.Conditions {
			if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
				detail = condition.Reason + ": " + condition.Message
			}
			if condition.Type == corev1.ContainersReady && condition.Status == corev1.ConditionTrue {
				return claim.Status.Phase == corev1.ClaimBound, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("prepare uploader (%s): %w", detail, err)
	}
	return nil
}

func (e *Execution) waitPipelineRun(ctx context.Context, pr *tektonv1.PipelineRun) (*tektonv1.PipelineRun, error) {
	var terminal *tektonv1.PipelineRun
	err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := e.clients.Tekton.TektonV1().PipelineRuns(pr.Namespace).Get(ctx, pr.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if current.UID != pr.UID {
			return false, fmt.Errorf("PipelineRun was replaced")
		}
		condition := current.Status.GetCondition(apis.ConditionSucceeded)
		if condition != nil && (condition.Status == corev1.ConditionTrue || condition.Status == corev1.ConditionFalse) {
			terminal = current
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("wait for PipelineRun %s: %w", pr.Name, err)
	}
	final, err := e.clients.Tekton.TektonV1().PipelineRuns(pr.Namespace).Get(ctx, pr.Name, metav1.GetOptions{})
	if err != nil {
		return terminal, fmt.Errorf("read final PipelineRun status: %w", err)
	}
	condition := final.Status.GetCondition(apis.ConditionSucceeded)
	if final.UID != pr.UID || condition == nil || (condition.Status != corev1.ConditionTrue && condition.Status != corev1.ConditionFalse) {
		return terminal, fmt.Errorf("terminal PipelineRun identity or status changed")
	}
	return final, nil
}

func (e *Execution) logs(ctx context.Context, name string) error {
	args := []string{"--kubeconfig", e.options.Kubeconfig, "--context", e.options.Context, "--namespace", e.options.Namespace, "pipelinerun", "logs", name, "--follow"}
	// #nosec G204 -- CheckTkn resolves the executable and arguments never pass through a shell
	cmd := exec.CommandContext(ctx, e.tkn, args...)
	cmd.Stdout = e.streams.Out
	cmd.Stderr = e.streams.ErrOut
	cmd.WaitDelay = 5 * time.Second
	return cmd.Run()
}

func uidDelete(uid types.UID) metav1.DeleteOptions {
	return metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
}

func (e *Execution) deleteUploader(ctx context.Context, pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}
	if err := e.clients.Kube.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, uidDelete(pod.UID)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete uploader: %w", err)
	}
	return wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		p, err := e.clients.Kube.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if p.UID != pod.UID {
			return false, fmt.Errorf("uploader Pod was replaced during deletion")
		}
		return false, nil
	})
}

func (e *Execution) cancel(ctx context.Context, pr *tektonv1.PipelineRun) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := e.clients.Tekton.TektonV1().PipelineRuns(pr.Namespace).Get(ctx, pr.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if current.UID != pr.UID {
			return fmt.Errorf("refusing to cancel a replaced PipelineRun")
		}
		condition := current.Status.GetCondition(apis.ConditionSucceeded)
		if condition != nil && (condition.Status == corev1.ConditionTrue || condition.Status == corev1.ConditionFalse) {
			return nil
		}
		current.Spec.Status = tektonv1.PipelineRunSpecStatusCancelled
		_, err = e.clients.Tekton.TektonV1().PipelineRuns(pr.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
}

func (e *Execution) cleanup(ctx context.Context, pr *tektonv1.PipelineRun, pvc *corev1.PersistentVolumeClaim, pod *corev1.Pod, failed bool) error {
	if err := e.deleteUploader(ctx, pod); err != nil {
		return err
	}
	if failed {
		if err := e.cancel(ctx, pr); err != nil {
			return fmt.Errorf("cancel PipelineRun: %w", err)
		}
	}
	if e.options.KeepResources {
		if failed {
			return e.waitTaskPods(ctx, pr, false)
		}
		return nil
	}
	options := uidDelete(pr.UID)
	propagation := metav1.DeletePropagationForeground
	options.PropagationPolicy = &propagation
	if err := e.clients.Tekton.TektonV1().PipelineRuns(pr.Namespace).Delete(ctx, pr.Name, options); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := e.clients.Tekton.TektonV1().PipelineRuns(pr.Namespace).Get(ctx, pr.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if current.UID != pr.UID {
			return false, fmt.Errorf("PipelineRun was replaced during cleanup")
		}
		return false, nil
	}); err != nil {
		return err
	}
	if err := e.waitTaskPods(ctx, pr, true); err != nil {
		return err
	}
	if pvc == nil {
		return nil
	}
	if err := e.clients.Kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, uidDelete(pvc.UID)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		claim, err := e.clients.Kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if claim.UID != pvc.UID {
			return false, fmt.Errorf("PVC was replaced during cleanup")
		}
		return false, nil
	})
}

func ownedBy(owners []metav1.OwnerReference, uid types.UID) bool {
	for _, owner := range owners {
		if owner.UID == uid {
			return true
		}
	}
	return false
}

func (e *Execution) waitTaskPods(ctx context.Context, pr *tektonv1.PipelineRun, deleted bool) error {
	return wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		runs, err := e.clients.Tekton.TektonV1().TaskRuns(pr.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "tekton.dev/pipelineRun=" + pr.Name})
		if err != nil {
			return false, err
		}
		remaining := false
		for _, tr := range runs.Items {
			if ownedBy(tr.OwnerReferences, pr.UID) {
				if deleted {
					remaining = true
				}
			}
		}
		pods, err := e.clients.Kube.CoreV1().Pods(pr.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "tekton.dev/pipelineRun=" + pr.Name})
		if err != nil {
			return false, err
		}
		for _, pod := range pods.Items {
			// A TaskRun may disappear before its Pod, so wait conservatively by the unique run label
			if deleted || (pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed) {
				remaining = true
			}
		}

		return !remaining, nil
	})
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

func (e *Execution) recovery() {
	target := "--kubeconfig " + shellQuote(e.options.Kubeconfig) + " --context " + shellQuote(e.options.Context) + " --namespace " + shellQuote(e.options.Namespace)
	fmt.Fprintf(e.streams.ErrOut, "Run identity: %s; source PVC: %s\nInspect: tkn %s pipelinerun describe %s\nLogs: tkn %s pipelinerun logs %s --follow\nCleanup: kubectl %s delete pipelinerun %s --cascade=foreground\n", e.pipelineRun.Name, e.pvc.Name, target, e.pipelineRun.Name, target, e.pipelineRun.Name, target, e.pipelineRun.Name)
}
