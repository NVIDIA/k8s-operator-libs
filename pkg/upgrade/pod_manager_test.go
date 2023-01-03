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
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
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

	var ctx context.Context
	var id string
	rand.Seed(time.Now().UnixNano())

	BeforeEach(func() {
		ctx = context.TODO()
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
			err := k8sClient.List(ctx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(4))

			manager := upgrade.NewPodManager(k8sInterface, upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder), log, nil, eventRecorder)
			err = manager.SchedulePodsRestart(ctx, restartPods)
			Expect(err).To(Succeed())

			podList = &corev1.PodList{}
			err = k8sClient.List(ctx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(1))

			// Check that pod not scheduled for restart is not deleted
			err = k8sClient.Get(ctx, types.NamespacedName{Name: noRestartPod.Name, Namespace: namespace.Name}, noRestartPod)
			Expect(err).To(Succeed())
		})
		It("should report an error on invalid input", func() {
			deletedPod := NewPod("deleted-pod", namespace.Name, node.Name).Create()
			deleteObj(deletedPod)

			podList := &corev1.PodList{}
			err := k8sClient.List(ctx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(0))

			manager := upgrade.NewPodManager(k8sInterface, upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder), log, nil, eventRecorder)
			err = manager.SchedulePodsRestart(ctx, []*corev1.Pod{deletedPod})
			Expect(err).To(HaveOccurred())
		})
		It("should not fail on empty input", func() {
			podList := &corev1.PodList{}
			err := k8sClient.List(ctx, podList)
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(0))

			manager := upgrade.NewPodManager(k8sInterface, upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder), log, nil, eventRecorder)
			err = manager.SchedulePodsRestart(ctx, []*corev1.Pod{})
			Expect(err).To(Succeed())
		})
	})

	Describe("ScheduleCheckOnPodCompletion", func() {
		It("should change the state of the node only after job completion", func() {
			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(ctx, node, upgrade.UpgradeStateWaitForJobsRequired)
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
			podList, err := k8sInterface.CoreV1().Pods("").List(ctx, listOptions)
			Expect(err).To(Succeed())
			Expect(podList.Items).NotTo(BeEmpty())

			podManagerConfig.WaitForCompletionSpec.PodSelector = "app=my-app"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, nil, eventRecorder)
			err = manager.ScheduleCheckOnPodCompletion(ctx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is changed to new state on workload pod completion
			node, err = provider.GetNode(ctx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStatePodDeletionRequired))
		})
		It("should not change the state of the node if workload pod is running", func() {
			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(ctx, node, upgrade.UpgradeStateWaitForJobsRequired)
			Expect(err).To(Succeed())

			// create pod to be running on testnode
			labels := map[string]string{"app": "my-app"}
			_ = NewPod("test-pod", namespace.Name, node.Name).WithLabels(labels).Create()

			// get pod scheduled for the job
			listOptions := metav1.ListOptions{LabelSelector: "app=my-app", FieldSelector: "spec.nodeName=" + node.Name}
			podList, err := k8sInterface.CoreV1().Pods("").List(ctx, listOptions)
			Expect(err).To(Succeed())
			Expect(podList.Items).NotTo(BeEmpty())

			podManagerConfig.WaitForCompletionSpec.PodSelector = "app=my-app"
			manager := upgrade.NewPodManager(k8sInterface, provider, log, nil, eventRecorder)
			err = manager.ScheduleCheckOnPodCompletion(ctx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is unchanged with workload pod running
			node, err = provider.GetNode(ctx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateWaitForJobsRequired))
		})
		It("should change the state of the node if workload pod is running and timeout is reached", func() {
			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(ctx, node, upgrade.UpgradeStateWaitForJobsRequired)
			Expect(err).To(Succeed())

			// create pod to be running on testnode
			labels := map[string]string{"app": "my-app"}
			_ = NewPod("test-pod", namespace.Name, node.Name).WithLabels(labels).Create()

			// get pod scheduled for the job
			listOptions := metav1.ListOptions{LabelSelector: "app=my-app", FieldSelector: "spec.nodeName=" + node.Name}
			podList, err := k8sInterface.CoreV1().Pods("").List(ctx, listOptions)
			Expect(err).To(Succeed())
			Expect(podList.Items).NotTo(BeEmpty())

			podManagerConfig.WaitForCompletionSpec.PodSelector = "app=my-app"
			podManagerConfig.WaitForCompletionSpec.TimeoutSecond = 30
			manager := upgrade.NewPodManager(k8sInterface, provider, log, nil, eventRecorder)
			err = manager.ScheduleCheckOnPodCompletion(ctx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is unchanged with workload pod running
			node, err = provider.GetNode(ctx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateWaitForJobsRequired))

			// verify annotation is added track the start time.
			Expect(isWaitForCompletionAnnotationPresent(node)).To(Equal(true))

			startTime := strconv.FormatInt(time.Now().Unix()-35, 10)
			provider.ChangeNodeUpgradeAnnotation(ctx, node, upgrade.GetWaitForPodCompletionStartTimeAnnotationKey(), startTime)

			podManagerConfig.Nodes = []*corev1.Node{node}

			err = manager.ScheduleCheckOnPodCompletion(ctx, &podManagerConfig)
			Expect(err).To(Succeed())

			// verify upgrade state is unchanged with workload pod running
			node, err = provider.GetNode(ctx, node.Name)
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

		It("should delete all standalone gpu pods with force", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
				NewPod(fmt.Sprintf("gpu-pod2-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/mig-1g.5gb", "1").Create(),
			}

			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(ctx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(ctx, &podManagerConfig)
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to delete pods and run to completion
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods)))

			// verify upgrade state
			node, err = provider.GetNode(ctx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateDrainRequired))
		})

		It("should fail to delete all standalone gpu pods without force", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
				NewPod(fmt.Sprintf("gpu-pod2-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/mig-1g.5gb", "1").Create(),
			}

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(ctx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(ctx, &podManagerConfig)
			// Note: SchedulePodEviction() will not return an error if issues were encountered
			// when deleting pods on a node. The node will be transitioned to the UpgradeFailed
			// state so upgrade can proceed with rest of nodes.
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to run to completion on pod eviction to update nodes states
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods) + len(gpuPods)))

			// verify upgrade state is set to UpgradeStateFailed
			node, err = provider.GetNode(ctx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateFailed))
		})

		It("should delete all standalone gpu pods using emptyDir when force=true and deleteEmptyDir=true ", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").Create(),
				NewPod(fmt.Sprintf("gpu-pod2-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/mig-1g.5gb", "1").Create(),
			}
			// create one gpu pod with an emptyDir volume
			gpuPods = append(gpuPods, NewPod("test-gpu-pod", namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").WithEmptyDir().Create())

			// initialize upgrade state of the node
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(ctx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			podManagerConfig.DeletionSpec.DeleteEmptyDir = true
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(ctx, &podManagerConfig)
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to delete pods and run to completion
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods)))

			// verify upgrade state
			node, err = provider.GetNode(ctx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateDrainRequired))
		})

		It("should fail to delete all standalone gpu pods with emptyDir when force=true and deleteEmptyDir=false", func() {
			gpuPods = []*corev1.Pod{
				NewPod(fmt.Sprintf("gpu-pod1-%s", id), namespace.Name, node.Name).WithResource("nvidia.com/gpu", "1").WithEmptyDir().Create(),
			}

			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			err := provider.ChangeNodeUpgradeState(ctx, node, upgrade.UpgradeStatePodDeletionRequired)
			Expect(err).To(Succeed())

			podManagerConfig.DeletionSpec.Force = true
			manager := upgrade.NewPodManager(k8sInterface, provider, log, gpuPodSpecFilter, eventRecorder)
			err = manager.SchedulePodEviction(ctx, &podManagerConfig)
			// Note: SchedulePodEviction() will not return an error if issues were encountered
			// when deleting pods on a node. The node will be transitioned to the UpgradeFailed
			// state so upgrade can proceed with rest of nodes.
			Expect(err).To(Succeed())

			// add a slight delay to let go routines to run to completion on pod eviction to update nodes states
			time.Sleep(100 * time.Millisecond)

			// check number of pods still running in namespace
			podList, err := k8sInterface.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			Expect(podList.Items).To(HaveLen(len(cpuPods) + len(gpuPods)))

			// verify upgrade state is set to UpgradeStateFailed
			node, err = provider.GetNode(ctx, node.Name)
			Expect(err).To(Succeed())
			Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateFailed))
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
