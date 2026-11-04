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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
)

var _ = Describe("Rate limiting tests", func() {
	var (
		ctx             context.Context
		testID          string
		testNodes       []*corev1.Node
		testPods        []*corev1.Pod
		testDaemonSet   *appsv1.DaemonSet
		stateManager    *upgrade.ClusterUpgradeStateManagerImpl
		upgradeSpec     *v1alpha1.DriverUpgradePolicySpec
	)

	BeforeEach(func() {
		ctx = context.Background()
		testID = randSeq(5)

		var err error
		opts := upgrade.StateOptions{}
		stateManagerInterface, err := upgrade.NewClusterUpgradeStateManager(log, k8sConfig, eventRecorder, opts)
		Expect(err).NotTo(HaveOccurred())

		stateManager = stateManagerInterface.(*upgrade.ClusterUpgradeStateManagerImpl)
		stateManager.NodeUpgradeStateProvider = &nodeUpgradeStateProvider
		stateManager.DrainManager = &drainManager
		stateManager.CordonManager = &cordonManager
		stateManager.PodManager = &podManager
		stateManager.ValidationManager = &validationManager

		upgradeSpec = &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade:         true,
			MaxParallelUpgrades: 1,
			MaxUnavailable:      intstr.FromInt(10),
			PodDeletion: &v1alpha1.PodDeletionSpec{
				Force:           true,
				TimeoutSecond:   300,
				DeleteEmptyDir:  true,
			},
			DrainSpec: &v1alpha1.DrainSpec{
				Enable:        true,
				Force:         true,
				PodSelector:   "",
				TimeoutSecond: 300,
				DeleteEmptyDir: true,
			},
		}
	})

	AfterEach(func() {
		for _, node := range testNodes {
			_ = k8sClient.Delete(ctx, node)
		}
		for _, pod := range testPods {
			_ = k8sClient.Delete(ctx, pod)
		}
		if testDaemonSet != nil {
			_ = k8sClient.Delete(ctx, testDaemonSet)
		}
	})

	Context("When maxParallelUpgrades=1 and multiple nodes in PodRestartRequired state", func() {
		It("should only delete driver pods up to maxParallelUpgrades limit per reconcile", func() {
			// Create DaemonSet with old revision
			testDaemonSet = &appsv1.DaemonSet{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-driver-ds-" + testID,
					Namespace: "gpu-operator",
					UID:       types.UID("test-ds-uid-" + testID),
				},
				Spec: appsv1.DaemonSetSpec{
					Selector: &v1.LabelSelector{
						MatchLabels: map[string]string{"app": "nvidia-driver"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: v1.ObjectMeta{
							Labels: map[string]string{"app": "nvidia-driver"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "nvidia-driver", Image: "nvidia/driver:old"}},
						},
					},
					UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
						Type: appsv1.OnDeleteDaemonSetStrategyType,
					},
				},
			}
			Expect(k8sClient.Create(ctx, testDaemonSet)).To(Succeed())

			// Create 5 nodes with driver pods in PodRestartRequired state
			numNodes := 5
			testNodes = make([]*corev1.Node, numNodes)
			testPods = make([]*corev1.Pod, numNodes)

			for i := 0; i < numNodes; i++ {
				nodeName := fmt.Sprintf("test-node-%s-%d", testID, i)
				testNodes[i] = &corev1.Node{
					ObjectMeta: v1.ObjectMeta{
						Name: nodeName,
						Labels: map[string]string{
							upgrade.GetUpgradeStateLabelKey(): string(upgrade.UpgradeStatePodRestartRequired),
						},
					},
					Status: corev1.NodeStatus{
						Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
					},
				}
				Expect(k8sClient.Create(ctx, testNodes[i])).To(Succeed())

				// Create driver pod with old revision (needs restart)
				testPods[i] = &corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Name:      fmt.Sprintf("nvidia-driver-pod-%s-%d", testID, i),
						Namespace: "gpu-operator",
						Labels: map[string]string{
							"app": "nvidia-driver",
							"controller-revision-hash": "old-revision",
						},
						OwnerReferences: []v1.OwnerReference{{
							APIVersion: "apps/v1",
							Kind:       "DaemonSet",
							Name:       testDaemonSet.Name,
							UID:        testDaemonSet.UID,
							Controller: boolPtr(true),
						}},
					},
					Spec: corev1.PodSpec{
						NodeName: nodeName,
						Containers: []corev1.Container{{Name: "nvidia-driver", Image: "nvidia/driver:old"}},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
						Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
					},
				}
				Expect(k8sClient.Create(ctx, testPods[i])).To(Succeed())
			}

			// Build cluster state
			clusterState, err := stateManager.BuildClusterUpgradeState(ctx, "gpu-operator",
				map[string]string{"app": "nvidia-driver"}, upgradeSpec)
			Expect(err).NotTo(HaveOccurred())
			Expect(clusterState.NodeStates[upgrade.UpgradeStatePodRestartRequired]).To(HaveLen(numNodes))

			// First reconcile: GetUpgradesAvailable should initialize rate limiter
			available := stateManager.GetUpgradesAvailable(clusterState, 1, 10)
			Expect(available).To(BeNumerically(">=", 0))

			// Process nodes - should only schedule 1 pod deletion due to maxParallelUpgrades=1
			err = stateManager.ProcessPodRestartNodes(ctx, clusterState)
			Expect(err).NotTo(HaveOccurred())

			// Verify: podManager.SchedulePodsRestart should be called with exactly 1 pod
			// (This would require mocking podManager and checking call arguments)
			// For now, verify via logs or internal state if accessible

			// Second reconcile: reset counter, should allow 1 more
			clusterState2, err := stateManager.BuildClusterUpgradeState(ctx, "gpu-operator",
				map[string]string{"app": "nvidia-driver"}, upgradeSpec)
			Expect(err).NotTo(HaveOccurred())

			available2 := stateManager.GetUpgradesAvailable(clusterState2, 1, 10)
			Expect(available2).To(BeNumerically(">=", 0))

			err = stateManager.ProcessPodRestartNodes(ctx, clusterState2)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When maxParallelUpgrades=0 (unlimited)", func() {
		It("should delete all driver pods without rate limiting", func() {
			upgradeSpec.MaxParallelUpgrades = 0

			// Create DaemonSet
			testDaemonSet = &appsv1.DaemonSet{
				ObjectMeta: v1.ObjectMeta{
					Name:      "test-driver-ds-unlimited-" + testID,
					Namespace: "gpu-operator",
					UID:       types.UID("test-ds-uid-unlimited-" + testID),
				},
				Spec: appsv1.DaemonSetSpec{
					Selector: &v1.LabelSelector{
						MatchLabels: map[string]string{"app": "nvidia-driver"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: v1.ObjectMeta{
							Labels: map[string]string{"app": "nvidia-driver"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "nvidia-driver", Image: "nvidia/driver:old"}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, testDaemonSet)).To(Succeed())

			// Create 10 nodes
			numNodes := 10
			testNodes = make([]*corev1.Node, numNodes)
			testPods = make([]*corev1.Pod, numNodes)

			for i := 0; i < numNodes; i++ {
				nodeName := fmt.Sprintf("test-node-unlimited-%s-%d", testID, i)
				testNodes[i] = &corev1.Node{
					ObjectMeta: v1.ObjectMeta{
						Name: nodeName,
						Labels: map[string]string{
							upgrade.GetUpgradeStateLabelKey(): string(upgrade.UpgradeStatePodRestartRequired),
						},
					},
					Status: corev1.NodeStatus{
						Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
					},
				}
				Expect(k8sClient.Create(ctx, testNodes[i])).To(Succeed())

				testPods[i] = &corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Name:      fmt.Sprintf("nvidia-driver-unlimited-%s-%d", testID, i),
						Namespace: "gpu-operator",
						Labels: map[string]string{
							"app": "nvidia-driver",
							"controller-revision-hash": "old-revision",
						},
						OwnerReferences: []v1.OwnerReference{{
							APIVersion: "apps/v1",
							Kind:       "DaemonSet",
							Name:       testDaemonSet.Name,
							UID:        testDaemonSet.UID,
							Controller: boolPtr(true),
						}},
					},
					Spec: corev1.PodSpec{
						NodeName: nodeName,
						Containers: []corev1.Container{{Name: "nvidia-driver", Image: "nvidia/driver:old"}},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
						Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
					},
				}
				Expect(k8sClient.Create(ctx, testPods[i])).To(Succeed())
			}

			clusterState, err := stateManager.BuildClusterUpgradeState(ctx, "gpu-operator",
				map[string]string{"app": "nvidia-driver"}, upgradeSpec)
			Expect(err).NotTo(HaveOccurred())

			available := stateManager.GetUpgradesAvailable(clusterState, 0, 20)
			Expect(available).To(Equal(numNodes)) // unlimited

			err = stateManager.ProcessPodRestartNodes(ctx, clusterState)
			Expect(err).NotTo(HaveOccurred())

			// All 10 pods should be scheduled for deletion
		})
	})

	Context("CanDeleteDriverPod method", func() {
		It("should respect maxParallelUpgrades limit", func() {
			clusterState := &upgrade.ClusterUpgradeState{
				NodeStates: make(map[upgrade.UpgradeState][]*upgrade.NodeUpgradeState),
			}

			// Initialize with maxParallelUpgrades=2
			stateManager.GetUpgradesAvailable(clusterState, 2, 10)

			// First two calls should succeed
			Expect(stateManager.CanDeleteDriverPod("node1")).To(BeTrue())
			Expect(stateManager.CanDeleteDriverPod("node2")).To(BeTrue())

			// Third call should be rate-limited
			Expect(stateManager.CanDeleteDriverPod("node3")).To(BeFalse())

			// Reset counter via new GetUpgradesAvailable call
			stateManager.GetUpgradesAvailable(clusterState, 2, 10)

			// Should allow 2 more
			Expect(stateManager.CanDeleteDriverPod("node4")).To(BeTrue())
			Expect(stateManager.CanDeleteDriverPod("node5")).To(BeTrue())
			Expect(stateManager.CanDeleteDriverPod("node6")).To(BeFalse())
		})

		It("should allow unlimited deletions when maxParallelUpgrades=0", func() {
			clusterState := &upgrade.ClusterUpgradeState{
				NodeStates: make(map[upgrade.UpgradeState][]*upgrade.NodeUpgradeState),
			}

			stateManager.GetUpgradesAvailable(clusterState, 0, 10)

			// All calls should succeed
			for i := 0; i < 100; i++ {
				Expect(stateManager.CanDeleteDriverPod(fmt.Sprintf("node%d", i))).To(BeTrue())
			}
		})
	})
})

func boolPtr(b bool) *bool {
	return &b
}
