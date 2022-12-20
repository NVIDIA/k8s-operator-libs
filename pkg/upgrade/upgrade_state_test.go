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

	v1alpha1 "github.com/NVIDIA/k8s-operator-libs/api"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/mocks"
	"github.com/NVIDIA/k8s-operator-libs/pkg/utils"
)

var _ = Describe("UpgradeStateManager tests", func() {
	var ctx context.Context
	var stateManager *upgrade.ClusterUpgradeStateManager

	BeforeEach(func() {
		ctx = context.TODO()
		// Create new ClusterUpgradeStateManager using mocked managers initialized in BeforeSuite()
		var err error
		stateManager, err = upgrade.NewClusterUpdateStateManager(log, k8sConfig, eventRecorder, nil)
		Expect(err).NotTo(HaveOccurred())
		stateManager.NodeUpgradeStateProvider = &nodeUpgradeStateProvider
		stateManager.DrainManager = &drainManager
		stateManager.CordonManager = &cordonManager
		stateManager.PodManager = &podManager
	})

	It("UpgradeStateManager should fail on nil currentState", func() {
		Expect(stateManager.ApplyState(ctx, nil, &v1alpha1.DriverUpgradePolicySpec{})).ToNot(Succeed())
	})
	It("UpgradeStateManager should not fail on nil upgradePolicy", func() {
		Expect(stateManager.ApplyState(ctx, &upgrade.ClusterUpgradeState{}, nil)).To(Succeed())
	})
	It("UpgradeStateManager should move up-to-date nodes to Done and outdated nodes to UpgradeRequired states", func() {
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

		Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
		Expect(getNodeUpgradeState(DoneToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
	})
	It("UpgradeStateManager should move outdated nodes to UpgradeRequired state and annotate node if unschedulable", func() {
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

		UnknownToUpgradeRequiredNode.Spec.Unschedulable = true
		DoneToUpgradeRequiredNode.Spec.Unschedulable = true

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

		Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
		Expect(getNodeUpgradeState(DoneToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))

		Expect(isUnschedulableAnnotationPresent(UnknownToUpgradeRequiredNode)).
			To(Equal(true))
		Expect(isUnschedulableAnnotationPresent(DoneToUpgradeRequiredNode)).
			To(Equal(true))
		Expect(isUnschedulableAnnotationPresent(UnknownToDoneNode)).
			To(Equal(false))
		Expect(isUnschedulableAnnotationPresent(DoneToDoneNode)).
			To(Equal(false))

	})
	It("UpgradeStateManager should schedule upgrade on all nodes if maxParallel upgrades is set to 0", func() {
		clusterState := upgrade.NewClusterUpgradeState()
		nodeStates := []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)},
		}

		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = nodeStates

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			// Unlimited upgrades
			MaxParallelUpgrades: 0,
		}

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

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade:         true,
			MaxParallelUpgrades: maxParallelUpgrades,
		}

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

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade:         true,
			MaxParallelUpgrades: maxParallelUpgrades,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: true,
			},
		}

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
	It("UpgradeStateManager should skip pod deletion if no filter is provided to PodManager at contruction", func() {
		ctx := context.TODO()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
		}

		policyWithNoDrainSpec := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		Expect(stateManager.ApplyState(ctx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
		for _, state := range clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] {
			Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStateDrainRequired))
		}
	})
	It("UpgradeStateManager should not skip pod deletion if a filter is provided to PodManager at contruction", func() {
		ctx := context.TODO()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
		}

		policyWithNoDrainSpec := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		podManagerMock := mocks.PodManager{}
		podManagerMock.
			On("GetPodDeletionFilter").
			Return(func() upgrade.PodDeletionFilter {
				return func(pod corev1.Pod) bool { return false }
			})
		podManagerMock.
			On("SchedulePodsRestart", mock.Anything, mock.Anything).
			Return(nil)
		stateManager.PodManager = &podManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
		for _, state := range clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] {
			Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStatePodDeletionRequired))
		}

	})
	It("UpgradeStateManager should skip drain if it's disabled by policy", func() {
		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
		}

		policyWithNoDrainSpec := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		policyWithDisabledDrain := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: false,
			},
		}

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
		skipDrainPodSelector := fmt.Sprintf("%s!=true", upgrade.GetUpgradeSkipDrainPodLabelKey())

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
		}

		policy := v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
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
		stateManager.DrainManager = &drainManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, &policy)).To(Succeed())

		policy.DrainSpec.PodSelector = "test-label=test-value"
		expectedDrainSpec.PodSelector = fmt.Sprintf("%s,%s", policy.DrainSpec.PodSelector, skipDrainPodSelector)
		Expect(stateManager.ApplyState(ctx, &clusterState, &policy)).To(Succeed())
	})
	It("UpgradeStateManager should fail if drain manager returns an error", func() {
		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: true,
			},
		}

		drainManagerMock := mocks.DrainManager{}
		drainManagerMock.
			On("ScheduleNodesDrain", mock.Anything, mock.Anything).
			Return(func(ctx context.Context, config *upgrade.DrainConfiguration) error {
				return errors.New("drain failed")
			})
		stateManager.DrainManager = &drainManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).ToNot(Succeed())
	})
	It("UpgradeStateManager should not restart pod if it's up to date or already terminating", func() {
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

		policy := &v1alpha1.DriverUpgradePolicySpec{
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
		stateManager.PodManager = &podManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
	})
	It("UpgradeStateManager should move pod to UncordonRequired state "+
		"if it's in PodRestart or UpgradeFailed, up to date and ready", func() {
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{Generation: 3}}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
			},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "3"}}}
		podRestartNode := NewNode("pod-restart-node").WithUpgradeState(upgrade.UpgradeStatePodRestartRequired).Create()
		upgradeFailedNode := NewNode("upgrade-failed-node").WithUpgradeState(upgrade.UpgradeStateFailed).Create()

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
				Node:            upgradeFailedNode,
				DriverPod:       pod,
				DriverDaemonSet: daemonSet,
			},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(podRestartNode)).To(Equal(upgrade.UpgradeStateUncordonRequired))
		Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(upgrade.UpgradeStateUncordonRequired))
	})
	It("UpgradeStateManager should move pod to UpgradeDone state "+
		"if it's in PodRestart or UpgradeFailed, driver pod is up-to-date and ready, and node was initially Unschedulable", func() {
		ctx := context.TODO()

		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{Generation: 3}}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
			},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{utils.PodTemplateGenerationLabel: "3"}}}
		podRestartNode := NewNode("pod-restart-node-unschedulable").
			WithUpgradeState(upgrade.UpgradeStatePodRestartRequired).
			WithAnnotations(map[string]string{upgrade.GetUpgradeInitialStateAnnotationKey(): "true"}).
			Unschedulable(true).
			Create()
		upgradeFailedNode := NewNode("upgrade-failed-node-unschedulable").
			WithUpgradeState(upgrade.UpgradeStateFailed).
			WithAnnotations(map[string]string{upgrade.GetUpgradeInitialStateAnnotationKey(): "true"}).
			Unschedulable(true).
			Create()

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
				Node:            upgradeFailedNode,
				DriverPod:       pod,
				DriverDaemonSet: daemonSet,
			},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(podRestartNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(upgrade.UpgradeStateDone))
		// unschedulable annotation should be removed
		Expect(isUnschedulableAnnotationPresent(podRestartNode)).To(Equal(false))
		Expect(isUnschedulableAnnotationPresent(upgradeFailedNode)).To(Equal(false))
	})
	It("UpgradeStateManager should uncordon UncordonRequired pod and finish upgrade", func() {
		node := nodeWithUpgradeState(upgrade.UpgradeStateUncordonRequired)

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateUncordonRequired] = []*upgrade.NodeUpgradeState{
			{
				Node: node,
			},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		cordonManagerMock := mocks.CordonManager{}
		cordonManagerMock.
			On("Uncordon", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, node *corev1.Node) error {
				Expect(node).To(Equal(node))
				return nil
			})
		stateManager.CordonManager = &cordonManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(node)).To(Equal(upgrade.UpgradeStateDone))
	})
	It("UpgradeStateManager should fail if cordonManager fails", func() {
		node := nodeWithUpgradeState(upgrade.UpgradeStateUncordonRequired)

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateUncordonRequired] = []*upgrade.NodeUpgradeState{
			{
				Node: node,
			},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		cordonManagerMock := mocks.CordonManager{}
		cordonManagerMock.
			On("Uncordon", mock.Anything, mock.Anything, mock.Anything).
			Return(func(ctx context.Context, node *corev1.Node) error {
				return errors.New("cordonManagerFailed")
			})
		stateManager.CordonManager = &cordonManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).ToNot(Succeed())
		Expect(getNodeUpgradeState(node)).ToNot(Equal(upgrade.UpgradeStateDone))
	})
})

func nodeWithUpgradeState(state string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Labels:      map[string]string{upgrade.GetUpgradeStateLabelKey(): state},
			Annotations: map[string]string{},
		},
	}
}
