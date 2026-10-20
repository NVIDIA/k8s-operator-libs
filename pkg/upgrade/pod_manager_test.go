/*
Copyright 2022 NVIDIA CORPORATION & AFFILIATES

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgrade_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
)

var _ = Describe("PodManager", func() {
	var node *corev1.Node
	var namespace *corev1.Namespace
	var podManagerConfig upgrade.PodManagerConfig
	var id string
	rand.Seed(time.Now().UnixNano())

	BeforeEach(func() {
		// generate random id for test
		id = randSeq(5)
		// create k8s objects
		node = createNode(fmt.Sprintf("node-%s", id))
		namespace = createNamespace(fmt.Sprintf("namespace-%s", id))
		// default PodManagerConfig
		podManagerConfig = upgrade.PodManagerConfig{
			WaitForCompletionSpec: &v1alpha1.WaitForCompletionSpec{
				PodSelector:   "",
				TimeoutSecond: 0,
			},
			Nodes: []*corev1.Node{node},
			DeletionSpec: &v1alpha1.PodDeletionSpec{
				Force:          false,
				TimeoutSecond:  300,
				DeleteEmptyDir: false,
			},
			DrainEnabled: false,
		}
	})

	Describe("SchedulePodsRestart", func() {
		It("should only delete pods passed as arg", func() {
			noRestartPod := NewPod("no-restart-pod", namespace.Name, node.Name).Create()
			restartPods := []*corev1.Pod{
				NewPod("restart-pod1", namespace.Name, node.Name).Create(),
				NewPod("restart-pod2", namespace.Name, node.Name).Create(),
				NewPod("restart-pod3", namespace.Name, node.Name).Create(),
			}

			podList := &corev1.PodList{}
			err := k8sClient.List(testCtx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(4))

			manager := upgrade.NewPodManager(k8sInterface, upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder), log, nil, eventRecorder)
			err = manager.SchedulePodsRestart(testCtx, restartPods)
			Expect(err).To(Succeed())

			podList = &corev1.PodList{}
			err = k8sClient.List(testCtx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(1))

			// Check that pod not scheduled for restart is not deleted
			err = k8sClient.Get(testCtx, types.NamespacedName{Name: noRestartPod.Name, Namespace: namespace.Name}, noRestartPod)
			Expect(err).To(Succeed())
		})
		It("should report an error on invalid input", func() {
			deletedPod := NewPod("deleted-pod", namespace.Name, node.Name).Create()
			deleteObj(deletedPod)

			podList := &corev1.PodList{}
			err := k8sClient.List(testCtx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(0))

			manager := upgrade.NewPodManager(k8sInterface, upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder), log, nil, eventRecorder)
			err = manager.SchedulePodsRestart(testCtx, []*corev1.Pod{deletedPod})
			Expect(err).To(HaveOccurred())
		})
		It("should not fail on empty input", func() {
			podList := &corev1.PodList{}
			err := k8sClient.List(testCtx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(0))

			manager := upgrade.NewPodManager(k8sInterface, upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder), log, nil, eventRecorder)
			err = manager.SchedulePodsRestart(testCtx, []*corev1.Pod{})
			Expect(err).To(Succeed())
		})
	})

	Describe("ScheduleCheckOnPodCompletion", func() {
		It("should change the state of the node only after job completion", func() {
			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStateWaitForJobsRequired)
			Expect(err).To(Succeed())

			// create pod to be running on testnode
			labels := map[string]string{"app": "my-app"}
			pod := NewPod("test-pod", namespace.Name, node.Name).WithLabels(labels).Create()

			// set pod status as completed.
			pod.Status.Phase = corev1.PodSucceeded
			err = updatePodStatus(pod)
			Expect(err).To(Succeed())

			// get pod with the selector
			listOptions := metav1.ListOptions{LabelSelector: "app=my-app", FieldSelector: "spec.nodeName=" + node.Name}
			podList, err := k8sInterface.CoreV1().Pods("").List(testCtx, listOptions)
			Expect(err).To(Succeed())
			Expect(podList.Items).NotTo(BeEmpty())

			podManagerConfig.WaitForCompletionSpec.PodSelector = "app=my-app"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, nil, eventRecorder)
			err = manager.ScheduleCheckOnPodCompletion(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is changed to new state on workload pod completion
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStatePodDeletionRequired))
			// verify annotation which tracks start time is not added.
			Expect(isWaitForCompletionAnnotationPresent(node)).To(Equal(false))
		})
		It("should not change the state of the node if workload pod is running", func() {
			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStateWaitForJobsRequired)
			Expect(err).To(Succeed())

			// create pod to be running on testnode
			labels := map[string]string{"app": "my-app"}
			_ = NewPod("test-pod", namespace.Name, node.Name).WithLabels(labels).Create()

			// get pod scheduled for the job
			listOptions := metav1.ListOptions{LabelSelector: "app=my-app", FieldSelector: "spec.nodeName=" + node.Name}
			podList, err := k8sInterface.CoreV1().Pods("").List(testCtx, listOptions)
			Expect(err).To(Succeed())
			Expect(podList.Items).NotTo(BeEmpty())

			podManagerConfig.WaitForCompletionSpec.PodSelector = "app=my-app"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, nil, eventRecorder)
			err = manager.ScheduleCheckOnPodCompletion(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is unchanged with workload pod running
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateWaitForJobsRequired))
			// verify annotation is added to track the start time.
			Expect(isWaitForCompletionAnnotationPresent(node)).To(Equal(false))
		})
		It("should change the state of the node if workload pod is running and timeout is reached", func() {
			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStateWaitForJobsRequired)
			Expect(err).To(Succeed())

			// create pod to be running on testnode
			labels := map[string]string{"app": "my-app"}
			_ = NewPod("test-pod", namespace.Name, node.Name).WithLabels(labels).Create()

			// get pod scheduled for the job
			listOptions := metav1.ListOptions{LabelSelector: "app=my-app", FieldSelector: "spec.nodeName=" + node.Name}
			podList, err := k8sInterface.CoreV1().Pods("").List(testCtx, listOptions)
			Expect(err).To(Succeed())
			Expect(podList.Items).NotTo(BeEmpty())

			podManagerConfig.WaitForCompletionSpec.PodSelector = "app=my-app"
			podManagerConfig.WaitForCompletionSpec.TimeoutSecond = 30
			manager := upgrade.NewPodManager(k8sInterface, provider, log, nil, eventRecorder)
			err = manager.ScheduleCheckOnPodCompletion(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is unchanged with workload pod running
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateWaitForJobsRequired))

			// verify annotation is added track the start time.
			Expect(isWaitForCompletionAnnotationPresent(node)).To(Equal(true))

			startTime := strconv.FormatInt(time.Now().Unix()-35, 10)
			provider.ChangeNodeUpgradeAnnotation(testCtx, node, upgrade.GetWaitForPodCompletionStartTimeAnnotationKey(), startTime)

			podManagerConfig.Nodes = []*corev1.Node{node}

			err = manager.ScheduleCheckOnPodCompletion(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is unchanged with workload pod running
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStatePodDeletionRequired))
			// verify annotation is removed to track the start time.
			Expect(isWaitForCompletionAnnotationPresent(node)).To(Equal(false))
		})
	})

	Describe("SchedulePodEviction", func() {
		var cpuPods []*corev1.Pod
		var gpuPods []*corev1.Pod

		BeforeEach(func() {
			cpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("cpu-pod-%s", id), namespace.Name, node.Name).Create(),
			}
		})

		It("should delete all standalone gpu pods with force"+
			" and drain should be skipped", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
				NewPod(fmt.Sprintf("gpu-pod2-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/mig-1g.5gb", "1").Create(),
			}

			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to delete pods and run to completion
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods)))

			// verify upgrade state
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		})

		It("should ignore pod labels when no podSelector is set (backward compatible)", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
			}
			// Labelled non-GPU pod that WOULD match a selector, but none is set.
			labelledPod := NewPod(fmt.Sprintf("labelled-pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"nvidia.com/gpu-driver-upgrade-evict": "true"}).Create()

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			// PodSelector intentionally left empty.
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// Only the gpu pod is evicted; the labelled non-gpu pod and cpu pod remain.
			Eventually(func() int {
				podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
				Expect(err).To(Succeed())
				return len(podList.Items)
			}, "5s", "100ms").Should(Equal(len(cpuPods) + 1))
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: labelledPod.Name, Namespace: namespace.Name}, labelledPod)).To(Succeed())
		})

		It("should delete pods matching the additional podSelector that do not request a GPU,"+
			" alongside gpu pods", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
			}
			// Runtime-direct consumer: no nvidia.com/gpu request, so the injected
			// gpuPodSpecFilter does not select it. It is reached only via the
			// additional podSelector.
			selectorPod := NewPod(fmt.Sprintf("runtime-gpu-pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"nvidia.com/gpu-driver-upgrade-evict": "true"}).Create()

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DeletionSpec.PodSelector = "nvidia.com/gpu-driver-upgrade-evict=true"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// Only the unrelated cpu pod should remain.
			Eventually(func() int {
				podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
				Expect(err).To(Succeed())
				return len(podList.Items)
			}, "5s", "100ms").Should(Equal(len(cpuPods)))

			// The selector-matched pod is evicted even though it requests no GPU.
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: selectorPod.Name, Namespace: namespace.Name}, selectorPod)).NotTo(Succeed())

			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		})

		It("should delete a pod that both requests a GPU and matches the podSelector,"+
			" without a count mismatch", func() {
			gpuPods = []*corev1.Pod{
				// Matches BOTH the gpu resource filter and the selector.
				NewPod(fmt.Sprintf("gpu-and-label-%s", id), namespace.Name, node.Name).
					WithResource("nvidia.com/gpu", "1").
					WithLabels(map[string]string{"nvidia.com/gpu-driver-upgrade-evict": "true"}).Create(),
				// Matches the selector only.
				NewPod(fmt.Sprintf("label-only-%s", id), namespace.Name, node.Name).
					WithLabels(map[string]string{"nvidia.com/gpu-driver-upgrade-evict": "true"}).Create(),
			}

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DeletionSpec.PodSelector = "nvidia.com/gpu-driver-upgrade-evict=true"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// Both pods evicted; the overlap pod is not double counted (which would
			// otherwise trip the "cannot delete all required pods" path).
			Eventually(func() int {
				podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
				Expect(err).To(Succeed())
				return len(podList.Items)
			}, "5s", "100ms").Should(Equal(len(cpuPods)))

			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		})

		It("should not delete pods that neither request a GPU nor match the podSelector", func() {
			// A labelled non-GPU pod that does NOT match the configured selector
			// must be left alone.
			keepPod := NewPod(fmt.Sprintf("keep-pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"app": "unrelated"}).Create()
			// A pod with no labels at all must also be left alone by a positive selector.
			barePod := NewPod(fmt.Sprintf("bare-pod-%s", id), namespace.Name, node.Name).Create()

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DeletionSpec.PodSelector = "nvidia.com/gpu-driver-upgrade-evict=true"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// No pod matches, so the node proceeds and every pod survives.
			Eventually(func() string {
				n, err := provider.GetNode(testCtx, node.Name)
				Expect(err).To(Succeed())
				return n.Labels[upgrade.GetUpgradeStateLabelKey()]
			}, "5s", "100ms").Should(Equal(upgrade.UpgradeStatePodRestartRequired))

			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods) + 2))
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: keepPod.Name, Namespace: namespace.Name}, keepPod)).To(Succeed())
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: barePod.Name, Namespace: namespace.Name}, barePod)).To(Succeed())
		})

		It("should not evict a podSelector-matched pod that has already completed", func() {
			// A Succeeded pod matches the label but is excluded by the phase guard,
			// mirroring how the GPU Operator's own filter only considers
			// Running/Pending pods.
			donePod := NewPod(fmt.Sprintf("done-pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"nvidia.com/gpu-driver-upgrade-evict": "true"}).Create()
			donePod.Status.Phase = corev1.PodSucceeded
			Expect(updatePodStatus(donePod)).To(Succeed())

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DeletionSpec.PodSelector = "nvidia.com/gpu-driver-upgrade-evict=true"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			Eventually(func() string {
				n, err := provider.GetNode(testCtx, node.Name)
				Expect(err).To(Succeed())
				return n.Labels[upgrade.GetUpgradeStateLabelKey()]
			}, "5s", "100ms").Should(Equal(upgrade.UpgradeStatePodRestartRequired))
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: donePod.Name, Namespace: namespace.Name}, donePod)).To(Succeed())
		})

		It("should move the node to drain-required when a podSelector-matched pod cannot be"+
			" force-deleted", func() {
			// Standalone (unreplicated) labelled pod. Without Force the drain helper
			// refuses it, so the pre-count and the deletable count disagree and the
			// node falls back to drain. This proves selector-matched pods take part
			// in the same consistency invariant as filter-matched pods.
			NewPod(fmt.Sprintf("runtime-gpu-pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"nvidia.com/gpu-driver-upgrade-evict": "true"}).Create()

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.PodSelector = "nvidia.com/gpu-driver-upgrade-evict=true"
			podManagerConfig.DrainEnabled = true
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			Eventually(func() string {
				n, err := provider.GetNode(testCtx, node.Name)
				Expect(err).To(Succeed())
				return n.Labels[upgrade.GetUpgradeStateLabelKey()]
			}, "5s", "100ms").Should(Equal(upgrade.UpgradeStateDrainRequired))
		})

		It("should treat a whitespace-only podSelector as empty", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
			}
			labelledPod := NewPod(fmt.Sprintf("labelled-pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"nvidia.com/gpu-driver-upgrade-evict": "true"}).Create()

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			// Whitespace-only selector must NOT be parsed as a match-everything selector.
			podManagerConfig.DeletionSpec.PodSelector = "   "
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// Only the gpu pod is evicted; the labelled non-gpu pod is not swept up.
			Eventually(func() int {
				podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
				Expect(err).To(Succeed())
				return len(podList.Items)
			}, "5s", "100ms").Should(Equal(len(cpuPods) + 1))
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: labelledPod.Name, Namespace: namespace.Name}, labelledPod)).To(Succeed())
		})

		It("should return an error when the additional podSelector is invalid", func() {
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.PodSelector = "invalid selector"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(HaveOccurred())
		})

		It("should fail to delete all standalone gpu pods without force,"+
			" and node should be moved to UpgradeStateFailed when drain is disabled", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
				NewPod(fmt.Sprintf("gpu-pod2-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/mig-1g.5gb", "1").Create(),
			}

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			podManagerConfig.DrainEnabled = false
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			// Note: SchedulePodEviction() will not return an error if issues were encountered
			// when deleting pods on a node. The node will be transitioned to the UpgradeFailed
			// state so upgrade can proceed with rest of nodes.
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to run to completion on pod eviction to update nodes states
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods) + len(gpuPods)))

			// verify upgrade state is set to UpgradeStateFailed
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateFailed))
		})

		It("should fail to delete all standalone gpu pods without force,"+
			" and node should be moved to UpgradeStateDrainRequired when drain is enabled", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
				NewPod(fmt.Sprintf("gpu-pod2-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/mig-1g.5gb", "1").Create(),
			}

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			podManagerConfig.DrainEnabled = true
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			// Note: SchedulePodEviction() will not return an error if issues were encountered
			// when deleting pods on a node.
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to run to completion on pod eviction to update nodes states
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods) + len(gpuPods)))

			// verify upgrade state is set to UpgradeStateDrainRequired
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateDrainRequired))
		})

		It("should delete all standalone gpu pods using emptyDir when force=true and deleteEmptyDir=true"+
			" and drain should be skipped", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
				NewPod(fmt.Sprintf("gpu-pod2-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/mig-1g.5gb", "1").Create(),
			}
			// create one gpu pod with an emptyDir volume
			gpuPods = append(gpuPods, NewPod("test-gpu-pod", namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").WithEmptyDir().Create())

			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DeletionSpec.DeleteEmptyDir = true
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to delete pods and run to completion
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods)))

			// verify upgrade state
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		})

		It("should fail to delete all standalone gpu pods with emptyDir when force=true and deleteEmptyDir=false,"+
			" and node should be moved to UpgradeStateFailed when drain is disabled", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").WithEmptyDir().Create(),
			}

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DrainEnabled = false
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			// Note: SchedulePodEviction() will not return an error if issues were encountered
			// when deleting pods on a node. The node will be transitioned to the UpgradeFailed
			// state so upgrade can proceed with rest of nodes.
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to run to completion on pod eviction to update nodes states
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods) + len(gpuPods)))

			// verify upgrade state is set to UpgradeStateFailed
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateFailed))
		})

		It("should fail to delete all standalone gpu pods with emptyDir when force=true and deleteEmptyDir=false,"+
			" and node should be moved to UpgradeStateDrainRequired when drain is enabled", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").WithEmptyDir().Create(),
			}

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DrainEnabled = true
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(testCtx, &podManagerConfig)
			// Note: SchedulePodEviction() will not return an error if issues were encountered
			// when deleting pods on a node.
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to run to completion on pod eviction to update nodes states
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(testCtx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods) + len(gpuPods)))

			// verify upgrade state is set to UpgradeStateDrainRequired
			node, err = provider.GetNode(testCtx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateDrainRequired))
		})
	})
})

// Example pod spec filter which returns true if an NVIDIA GPU
// is allocated to any container in the pod provided as input.
func gpuPodSpecFilter(pod corev1.Pod) bool {
	gpuInResourceList := func(rl corev1.ResourceList) bool {
		for resourceName := range rl {
			str := string(resourceName)
			if strings.HasPrefix(str, "nvidia.com/gpu") || strings.HasPrefix(str, "nvidia.com/mig-") {
				return true
			}
		}
		return false
	}

	for _, c := range pod.Spec.Containers {
		if gpuInResourceList(c.Resources.Limits) || gpuInResourceList(c.Resources.Requests) {
			return true
		}
	}
	return false
}
