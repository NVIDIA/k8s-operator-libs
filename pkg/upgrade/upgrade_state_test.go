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
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/NVIDIA/operator-libs/pkg/upgrade"
	"github.com/NVIDIA/operator-libs/pkg/upgrade/mocks"
	"github.com/NVIDIA/operator-libs/pkg/utils"
)

var _ = Describe("UpgradeStateManager tests", func() {
	It("UpgradeStateManager should fail on nil currentState", func() {
		ctx := context.TODO()

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, nil, &upgrade.DriverUpgradePolicySpec{})).ToNot(Succeed())
	})
	It("UpgradeStateManager should not fail on nil upgradePolicy", func() {
		ctx := context.TODO()

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &upgrade.ClusterUpgradeState{}, nil)).To(Succeed())
	})
	It("UpgradeStateManager should move up-to-date nodes to Done and outdated nodes to UpgradeRequired states", func() {
		ctx := context.TODO()

		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{Generation: 2}}
		upToDatePod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "2"}}}
		outdatedPod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "1"}}}

		UnknownToDoneNode := nodeWithUpgradeState("")
		UnknownToUpgradeRequiredNode := nodeWithUpgradeState("")
		DoneToDoneNode := nodeWithUpgradeState(upgrade.UpgradeStateDone)
		DoneToUpgradeRequiredNode := nodeWithUpgradeState(upgrade.UpgradeStateDone)

		clusterState := upgrade.NewClusterUpgradeState()
		unknownNodes := []*upgrade.NodeUpgradeState{
			{Node: UnknownToDoneNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet},
			{Node: UnknownToUpgradeRequiredNode, DriverPod: outdatedPod, DriverDaemonSet: daemonSet},
		}
		doneNodes := []*upgrade.NodeUpgradeState{
			{Node: DoneToDoneNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet},
			{Node: DoneToUpgradeRequiredNode, DriverPod: outdatedPod, DriverDaemonSet: daemonSet},
		}
		clusterState.NodeStates[""] = unknownNodes
		clusterState.NodeStates[upgrade.UpgradeStateDone] = doneNodes

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, &upgrade.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
		Expect(getNodeUpgradeState(DoneToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
	})
	It("UpgradeStateManager should schedule upgrade on all nodes if maxParallel upgrades is set to 0", func() {
		ctx := context.TODO()

		clusterState := upgrade.NewClusterUpgradeState()
		nodeStates := []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
		}

		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = nodeStates

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			// Unlimited upgrades
			MaxParallelUpgrades: 0,
		}

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		stateCount := make(map[string]int)
		for i := range nodeStates {
			state := getNodeUpgradeState(clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired][i].Node)
			stateCount[state]++
		}
		Expect(stateCount[upgrade.UpgradeStateUpgradeRequired]).To(Equal(0))
		Expect(stateCount[upgrade.UpgradeStateCordonRequired]).To(Equal(len(nodeStates)))
	})
	It("UpgradeStateManager should start upgrade on limited amount of nodes "+
		"if maxParallel upgrades is less than node count", func() {
		ctx := context.TODO()

		const maxParallelUpgrades = 3

		clusterState := upgrade.NewClusterUpgradeState()
		nodeStates := []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
		}

		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = nodeStates

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade:         true,
			MaxParallelUpgrades: maxParallelUpgrades,
		}

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		stateCount := make(map[string]int)
		for i := range nodeStates {
			state := getNodeUpgradeState(nodeStates[i].Node)
			stateCount[state]++
		}
		Expect(stateCount[upgrade.UpgradeStateUpgradeRequired]).To(Equal(2))
		Expect(stateCount[upgrade.UpgradeStateCordonRequired]).To(Equal(maxParallelUpgrades))
	})
	It("UpgradeStateManager should start additional upgrades if maxParallelUpgrades limit is not reached", func() {
		ctx := context.TODO()

		const maxParallelUpgrades = 4

		upgradeRequiredNodes := []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
		}
		cordonRequiredNodes := []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateCordonRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateCordonRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateCordonRequired)},
		}
		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = upgradeRequiredNodes
		clusterState.NodeStates[upgrade.UpgradeStateCordonRequired] = cordonRequiredNodes

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade:         true,
			MaxParallelUpgrades: maxParallelUpgrades,
			DrainSpec: &upgrade.DrainSpec{
				Enable: true,
			},
		}

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		stateCount := make(map[string]int)
		for _, state := range append(upgradeRequiredNodes, cordonRequiredNodes...) {
			state := getNodeUpgradeState(state.Node)
			stateCount[state]++
		}
		Expect(stateCount[upgrade.UpgradeStateUpgradeRequired]).To(Equal(1))
		Expect(stateCount[upgrade.UpgradeStateCordonRequired] +
			stateCount[upgrade.UpgradeStateWaitForJobsRequired]).To(Equal(4))
	})
	It("UpgradeStateManager should skip drain if it's disabled by policy", func() {
		ctx := context.TODO()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
		}

		policyWithNoDrainSpec := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		policyWithDisabledDrain := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &upgrade.DrainSpec{
				Enable: false,
			},
		}

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
		for _, state := range clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] {
			Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		}

		clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
		}
		Expect(stateManager.ApplyState(ctx, &clusterState, policyWithDisabledDrain)).To(Succeed())
		for _, state := range clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] {
			Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		}
	})
	It("UpgradeStateManager should schedule drain for UpgradeStateDrainRequired nodes and pass drain config", func() {
		ctx := context.TODO()

		skipDrainPodSelector := fmt.Sprintf("%s!=true", upgrade.GetUpgradeSkipDrainPodLabelKey())

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
		}

		policy := upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &upgrade.DrainSpec{
				Enable: true,
			},
		}

		// Upgrade state manager should add the pod selector for skipping network-operator pods
		expectedDrainSpec := *policy.DrainSpec
		expectedDrainSpec.PodSelector = skipDrainPodSelector

		drainManagerMock := mocks.DrainManager{}
		drainManagerMock.
			On("ScheduleNodesDrain", mock.Anything, mock.Anything).
			Return(func(ctx context.Context, config *upgrade.DrainConfiguration) error {
				Expect(config.Spec).To(Equal(&expectedDrainSpec))
				Expect(config.Nodes).To(HaveLen(3))
				return nil
			})
		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManagerMock, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, &policy)).To(Succeed())

		policy.DrainSpec.PodSelector = "test-label=test-value"
		expectedDrainSpec.PodSelector = fmt.Sprintf("%s,%s", policy.DrainSpec.PodSelector, skipDrainPodSelector)
		Expect(stateManager.ApplyState(ctx, &clusterState, &policy)).To(Succeed())
	})
	It("UpgradeStateManager should fail if drain manager returns an error", func() {
		ctx := context.TODO()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
		}

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &upgrade.DrainSpec{
				Enable: true,
			},
		}

		drainManagerMock := mocks.DrainManager{}
		drainManagerMock.
			On("ScheduleNodesDrain", mock.Anything, mock.Anything).
			Return(func(ctx context.Context, config *upgrade.DrainConfiguration) error {
				return errors.New("drain failed")
			})
		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManagerMock, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).ToNot(Succeed())
	})
	It("UpgradeStateManager should not restart pod if it's up to date or already terminating", func() {
		ctx := context.TODO()

		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{Generation: 3}}
		upToDatePod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "3"}}}
		outdatedRunningPod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "2"}}}
		outdatedTerminatingPod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "1"}}}
		now := v1.Now()
		outdatedTerminatingPod.ObjectMeta.DeletionTimestamp = &now

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStatePodRestartRequired] = []*upgrade.NodeUpgradeState{
			{
				Node:            nodeWithUpgradeState(upgrade.UpgradeStatePodRestartRequired),
				DriverPod:       upToDatePod,
				DriverDaemonSet: daemonSet,
			},
			{
				Node:            nodeWithUpgradeState(upgrade.UpgradeStatePodRestartRequired),
				DriverPod:       outdatedRunningPod,
				DriverDaemonSet: daemonSet,
			},
			{
				Node:            nodeWithUpgradeState(upgrade.UpgradeStatePodRestartRequired),
				DriverPod:       outdatedTerminatingPod,
				DriverDaemonSet: daemonSet,
			},
		}

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		podManagerMock := mocks.PodManager{}
		podManagerMock.
			On("SchedulePodsRestart", mock.Anything, mock.Anything).
			Return(func(ctx context.Context, podsToDelete []*corev1.Pod) error {
				Expect(podsToDelete).To(HaveLen(1))
				Expect(podsToDelete[0]).To(Equal(outdatedRunningPod))
				return nil
			})
		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManagerMock, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
	})
	It("UpgradeStateManager should move pod to UncordonRequired state"+
		"if it's in PodRestart or DrainFailed, up to date and ready", func() {
		ctx := context.TODO()

		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{Generation: 3}}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
			},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "3"}}}
		podRestartNode := nodeWithUpgradeState(upgrade.UpgradeStatePodRestartRequired)
		drainFailedNode := nodeWithUpgradeState(upgrade.UpgradeStateFailed)

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStatePodRestartRequired] = []*upgrade.NodeUpgradeState{
			{
				Node:            podRestartNode,
				DriverPod:       pod,
				DriverDaemonSet: daemonSet,
			},
		}
		clusterState.NodeStates[upgrade.UpgradeStateFailed] = []*upgrade.NodeUpgradeState{
			{
				Node:            drainFailedNode,
				DriverPod:       pod,
				DriverDaemonSet: daemonSet,
			},
		}

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManager, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(podRestartNode)).To(Equal(upgrade.UpgradeStateUncordonRequired))
		Expect(getNodeUpgradeState(drainFailedNode)).To(Equal(upgrade.UpgradeStateUncordonRequired))
	})
	It("UpgradeStateManager should uncordon UncordonRequired pod and finish upgrade", func() {
		ctx := context.TODO()

		node := nodeWithUpgradeState(upgrade.UpgradeStateUncordonRequired)

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateUncordonRequired] = []*upgrade.NodeUpgradeState{
			{
				Node: node,
			},
		}

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		cordonManagerMock := mocks.CordonManager{}
		cordonManagerMock.
			On("Uncordon", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, node *corev1.Node) error {
				Expect(node).To(Equal(node))
				return nil
			})

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManagerMock, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(node)).To(Equal(upgrade.UpgradeStateDone))
	})
	It("UpgradeStateManager should fail if cordonManager fails", func() {
		ctx := context.TODO()

		node := nodeWithUpgradeState(upgrade.UpgradeStateUncordonRequired)

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateUncordonRequired] = []*upgrade.NodeUpgradeState{
			{
				Node: node,
			},
		}

		policy := &upgrade.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		cordonManagerMock := mocks.CordonManager{}
		cordonManagerMock.
			On("Uncordon", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, node *corev1.Node) error {
				return errors.New("cordonManagerFailed")
			})

		stateManager := upgrade.NewClusterUpdateStateManager(
			&drainManager, &podManager, &cordonManagerMock, &nodeUpgradeStateProvider, log, k8sClient, k8sInterface, eventRecorder)
		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).ToNot(Succeed())
		Expect(getNodeUpgradeState(node)).ToNot(Equal(upgrade.UpgradeStateDone))
	})
})

func nodeWithUpgradeState(state string) *corev1.Node {
	return &corev1.Node{ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.GetUpgradeStateLabelKey(): state}}}
}
