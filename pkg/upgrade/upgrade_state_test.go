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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	v1alpha1 "github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/mocks"
)

var _ = Describe("UpgradeStateManager tests", func() {
	var ctx context.Context
	var id string
	var stateManager *upgrade.ClusterUpgradeStateManagerImpl

	BeforeEach(func() {
		ctx = context.TODO()
		id = randSeq(5)
		// Create new ClusterUpgradeStateManagerImpl using mocked managers initialized in BeforeSuite()
		var err error
		stateManagerInterface, err := upgrade.NewClusterUpgradeStateManager(log, k8sConfig, eventRecorder)
		Expect(err).NotTo(HaveOccurred())
		stateManager, _ = stateManagerInterface.(*upgrade.ClusterUpgradeStateManagerImpl)
		stateManager.NodeUpgradeStateProvider = &nodeUpgradeStateProvider
		stateManager.DrainManager = &drainManager
		stateManager.CordonManager = &cordonManager
		stateManager.PodManager = &podManager
		stateManager.ValidationManager = &validationManager

	})

	It("UpgradeStateManager should fail on nil currentState", func() {
		Expect(stateManager.ApplyState(ctx, nil, &v1alpha1.DriverUpgradePolicySpec{})).ToNot(Succeed())
	})
	It("UpgradeStateManager should not fail on nil upgradePolicy", func() {
		Expect(stateManager.ApplyState(ctx, &upgrade.ClusterUpgradeState{}, nil)).To(Succeed())
	})
	It("UpgradeStateManager should move up-to-date nodes to Done and outdated nodes to UpgradeRequired states", func() {
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		upToDatePod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
		outdatedPod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-outadated"}}}

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

		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		upToDatePod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
		outdatedPod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}

		UnknownToDoneNode := NewNode(fmt.Sprintf("node1-%s", id)).Create()
		UnknownToUpgradeRequiredNode := NewNode(fmt.Sprintf("node2-%s", id)).Unschedulable(true).Create()
		DoneToDoneNode := NewNode(fmt.Sprintf("node3-%s", id)).WithUpgradeState(upgrade.UpgradeStateDone).Create()
		DoneToUpgradeRequiredNode := NewNode(fmt.Sprintf("node4-%s", id)).WithUpgradeState(upgrade.UpgradeStateDone).Unschedulable(true).Create()

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

		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

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
	It("UpgradeStateManager should move up-to-date nodes with safe driver loading annotation "+
		"to UpgradeRequired state", func() {
		ctx := context.TODO()

		safeLoadAnnotationKey := upgrade.GetUpgradeDriverWaitForSafeLoadAnnotationKey()
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		upToDatePod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}

		waitForSafeLoadNode := NewNode(fmt.Sprintf("node1-%s", id)).
			WithAnnotations(map[string]string{safeLoadAnnotationKey: "true"}).
			Create()
		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateDone] = []*upgrade.NodeUpgradeState{{
			Node: waitForSafeLoadNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet,
		}}

		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

		Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(waitForSafeLoadNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
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
	It("UpgradeStateManager should schedule upgrade all nodes if maxParallel upgrades is set to 0 and maxUnavailable is set to 100%", func() {
		clusterState := upgrade.NewClusterUpgradeState()
		nodeStates := []*upgrade.NodeUpgradeState{
			{Node: NewNode("node1").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node2").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node3").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node4").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
			{Node: NewNode("node5").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
		}

		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = nodeStates

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			// Unlimited upgrades
			MaxParallelUpgrades: 0,
			// Unlimited unavailable
			MaxUnavailable: &intstr.IntOrString{Type: intstr.String, StrVal: "100%"},
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
	It("UpgradeStateManager should schedule upgrade based on maxUnavailable constraint if maxParallel upgrades is set to 0 and maxUnavailable is set to 50%", func() {
		clusterState := upgrade.NewClusterUpgradeState()
		nodeStates := []*upgrade.NodeUpgradeState{
			{Node: NewNode("node1").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node2").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node3").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node4").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
			{Node: NewNode("node5").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
		}

		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = nodeStates

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			// Unlimited upgrades
			MaxParallelUpgrades: 0,
			MaxUnavailable:      &intstr.IntOrString{Type: intstr.String, StrVal: "50%"},
		}

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		stateCount := make(map[string]int)
		for i := range nodeStates {
			state := getNodeUpgradeState(clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired][i].Node)
			stateCount[state]++
		}
		Expect(stateCount[upgrade.UpgradeStateUpgradeRequired]).To(Equal(2))
		Expect(stateCount[upgrade.UpgradeStateCordonRequired]).To(Equal(3))
	})
	It("UpgradeStateManager should schedule upgrade based on 50% maxUnavailable, with some unavailable nodes already upgraded", func() {
		clusterState := upgrade.NewClusterUpgradeState()
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		upToDatePod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}

		upgradeRequiredNodes := []*upgrade.NodeUpgradeState{
			{Node: NewNode("node1").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node2").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node3").WithUpgradeState(upgrade.UpgradeStateUpgradeRequired).Node},
		}
		upgradeDoneNodes := []*upgrade.NodeUpgradeState{
			{
				Node:            NewNode("node4").WithUpgradeState(upgrade.UpgradeStateDone).Unschedulable(true).Node,
				DriverPod:       upToDatePod,
				DriverDaemonSet: daemonSet,
			},
			{
				Node:            NewNode("node5").WithUpgradeState(upgrade.UpgradeStateDone).Unschedulable(true).Node,
				DriverPod:       upToDatePod,
				DriverDaemonSet: daemonSet,
			},
		}
		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = upgradeRequiredNodes
		clusterState.NodeStates[upgrade.UpgradeStateDone] = upgradeDoneNodes

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			// Unlimited upgrades
			MaxParallelUpgrades: 0,
			MaxUnavailable:      &intstr.IntOrString{Type: intstr.String, StrVal: "50%"},
		}

		podManagerMock := mocks.PodManager{}
		podManagerMock.
			On("SchedulePodsRestart", mock.Anything, mock.Anything).
			Return(func(ctx context.Context, podsToDelete []*corev1.Pod) error {
				Expect(podsToDelete).To(HaveLen(0))
				return nil
			})
		podManagerMock.
			On("GetPodControllerRevisionHash", mock.Anything, mock.Anything).
			Return(
				func(ctx context.Context, pod *corev1.Pod) string {
					return pod.Labels[upgrade.PodControllerRevisionHashLabelKey]
				},
				func(ctx context.Context, pod *corev1.Pod) error {
					return nil
				},
			)
		podManagerMock.
			On("GetDaemonsetControllerRevisionHash", mock.Anything, mock.Anything, mock.Anything).
			Return("test-hash-12345", nil)
		stateManager.PodManager = &podManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		stateCount := make(map[string]int)
		for i := range upgradeRequiredNodes {
			state := getNodeUpgradeState(clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired][i].Node)
			stateCount[state]++
		}
		for i := range upgradeDoneNodes {
			state := getNodeUpgradeState(clusterState.NodeStates[upgrade.UpgradeStateDone][i].Node)
			stateCount[state]++
		}
		// check if already upgraded node states are not changed
		Expect(stateCount[upgrade.UpgradeStateDone]).To(Equal(2))
		// expect only single node to move to next state as upgradesUnavailble = maxUnavailable(3) - currentlyUnavailable(2)
		Expect(stateCount[upgrade.UpgradeStateCordonRequired]).To(Equal(1))
		// remaining nodes to be in same original state
		Expect(stateCount[upgrade.UpgradeStateUpgradeRequired]).To(Equal(2))
	})
	It("UpgradeStateManager should start upgrade on limited amount of nodes "+
		"if maxParallel upgrades  and maxUnavailable are less than node count", func() {
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
			MaxUnavailable:      &intstr.IntOrString{Type: intstr.Int, IntVal: 2},
		}

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		stateCount := make(map[string]int)
		for i := range nodeStates {
			state := getNodeUpgradeState(nodeStates[i].Node)
			stateCount[state]++
		}
		Expect(stateCount[upgrade.UpgradeStateUpgradeRequired]).To(Equal(3))
		// only maxUnavailable nodes should progress to next state
		Expect(stateCount[upgrade.UpgradeStateCordonRequired]).To(Equal(2))
	})
	It("UpgradeStateManager should start additional upgrades if maxParallelUpgrades and maxUnavailable limits are not reached", func() {
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
			MaxUnavailable:      &intstr.IntOrString{Type: intstr.Int, IntVal: 4},
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
	It("UpgradeStateManager should start additional upgrades if maxParallelUpgrades and maxUnavailable limits are not reached", func() {
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
			MaxUnavailable:      &intstr.IntOrString{Type: intstr.Int, IntVal: 4},
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

		filter := func(pod corev1.Pod) bool { return false }
		stateManager = stateManager.WithPodDeletionEnabled(filter).(*upgrade.ClusterUpgradeStateManagerImpl)
		Expect(stateManager.IsPodDeletionEnabled()).To(Equal(true))

		Expect(stateManager.ApplyState(ctx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
		for _, state := range clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] {
			Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStatePodDeletionRequired))
		}

	})
	It("UpgradeStateManager should not attempt to delete pods if pod deletion is disabled", func() {
		ctx := context.TODO()

		clusterState := upgrade.NewClusterUpgradeState()
		nodes := []*upgrade.NodeUpgradeState{
			{Node: nodeWithUpgradeState(upgrade.UpgradeStatePodDeletionRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStatePodDeletionRequired)},
			{Node: nodeWithUpgradeState(upgrade.UpgradeStatePodDeletionRequired)},
		}

		clusterState.NodeStates[upgrade.UpgradeStatePodDeletionRequired] = nodes

		policyWithNoPodDeletionSpec := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		podEvictionCalled := false

		podManagerMock := mocks.PodManager{}
		podManagerMock.
			On("SchedulePodEviction", mock.Anything, mock.Anything).
			Return(func(ctx context.Context, config *upgrade.PodManagerConfig) error {
				podEvictionCalled = true
				return nil
			}).
			On("SchedulePodsRestart", mock.Anything, mock.Anything).
			Return(func(ctx context.Context, pods []*corev1.Pod) error {
				return nil
			})
		stateManager.PodManager = &podManagerMock

		Eventually(podEvictionCalled).ShouldNot(Equal(true))

		Expect(stateManager.ApplyState(ctx, &clusterState, policyWithNoPodDeletionSpec)).To(Succeed())
		for _, state := range nodes {
			Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStateDrainRequired))
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
		expectedDrainSpec.PodSelector = policy.DrainSpec.PodSelector
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
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		upToDatePod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
		outdatedRunningPod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}
		outdatedTerminatingPod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}
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
		podManagerMock.
			On("GetPodControllerRevisionHash", mock.Anything, mock.Anything).
			Return(
				func(ctx context.Context, pod *corev1.Pod) string {
					return pod.Labels[upgrade.PodControllerRevisionHashLabelKey]
				},
				func(ctx context.Context, pod *corev1.Pod) error {
					return nil
				},
			)
		podManagerMock.
			On("GetDaemonsetControllerRevisionHash", mock.Anything, mock.Anything, mock.Anything).
			Return("test-hash-12345", nil)
		stateManager.PodManager = &podManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
	})
	It("UpgradeStateManager should unblock loading of the driver instead of restarting the Pod when node "+
		"is waiting for safe driver loading", func() {
		safeLoadAnnotation := upgrade.GetUpgradeDriverWaitForSafeLoadAnnotationKey()
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}

		upToDatePod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}

		waitForSafeLoadNode := NewNode(fmt.Sprintf("node1-%s", id)).
			WithUpgradeState(upgrade.UpgradeStatePodRestartRequired).
			WithAnnotations(map[string]string{safeLoadAnnotation: "true"}).
			Create()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStatePodRestartRequired] = []*upgrade.NodeUpgradeState{
			{
				Node:            waitForSafeLoadNode,
				DriverPod:       upToDatePod,
				DriverDaemonSet: daemonSet,
			},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: waitForSafeLoadNode.Name}, waitForSafeLoadNode)).
			NotTo(HaveOccurred())
		Expect(waitForSafeLoadNode.Annotations[safeLoadAnnotation]).To(BeEmpty())
	})
	It("UpgradeStateManager should move pod to UncordonRequired state "+
		"if it's in PodRestart or UpgradeFailed, up to date and ready", func() {
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
			},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
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

		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
			},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
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

		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(podRestartNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(upgrade.UpgradeStateDone))
		// unschedulable annotation should be removed
		Expect(isUnschedulableAnnotationPresent(podRestartNode)).To(Equal(false))
		Expect(isUnschedulableAnnotationPresent(upgradeFailedNode)).To(Equal(false))
	})
	It("UpgradeStateManager should move pod to UpgradeFailed state "+
		"if it's in PodRestart and driver pod is failing with repeated restarts", func() {
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		// pod1, mainCtr not Ready w/ no repeated restarts
		pod1 := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: false, RestartCount: 0}},
			},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}},
		}
		// pod2, initCtr finished, mainCtr not Ready w/ no repeated restarts
		pod2 := pod1.DeepCopy()
		pod2.Status.InitContainerStatuses = []corev1.ContainerStatus{{Ready: true, RestartCount: 0}}
		// pod3, initCtr finished, mainCtr not Ready w/ repeated restarts
		pod3 := pod2.DeepCopy()
		pod3.Status.ContainerStatuses[0].RestartCount = 11
		// pod4, initCtr repeated restarts, mainCtr not Ready w/ no repeated restarts
		pod4 := pod1.DeepCopy()
		pod4.Status.InitContainerStatuses = []corev1.ContainerStatus{{Ready: false, RestartCount: 11}}

		nodes := make([]*corev1.Node, 4)
		for i := 0; i < len(nodes); i++ {
			nodes[i] = NewNode(fmt.Sprintf("node%d-%s", i, id)).
				WithUpgradeState(upgrade.UpgradeStatePodRestartRequired).
				Create()
		}

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStatePodRestartRequired] = []*upgrade.NodeUpgradeState{
			{Node: nodes[0], DriverPod: pod1, DriverDaemonSet: daemonSet},
			{Node: nodes[1], DriverPod: pod2, DriverDaemonSet: daemonSet},
			{Node: nodes[2], DriverPod: pod3, DriverDaemonSet: daemonSet},
			{Node: nodes[3], DriverPod: pod4, DriverDaemonSet: daemonSet},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(nodes[0])).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		Expect(getNodeUpgradeState(nodes[1])).To(Equal(upgrade.UpgradeStatePodRestartRequired))
		Expect(getNodeUpgradeState(nodes[2])).To(Equal(upgrade.UpgradeStateFailed))
		Expect(getNodeUpgradeState(nodes[3])).To(Equal(upgrade.UpgradeStateFailed))
	})
	It("UpgradeStateManager should move pod to UpgradeValidationRequired state "+
		"if it's in PodRestart, driver pod is up-to-date and ready, and validation is enabled", func() {
		ctx := context.TODO()

		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
			},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-12345"}},
		}
		podRestartNode := NewNode(fmt.Sprintf("node1-%s", id)).
			WithUpgradeState(upgrade.UpgradeStatePodRestartRequired).
			Create()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStatePodRestartRequired] = []*upgrade.NodeUpgradeState{
			{
				Node:            podRestartNode,
				DriverPod:       pod,
				DriverDaemonSet: daemonSet,
			},
		}
		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		stateManager = stateManager.WithValidationEnabled("app=validator").(*upgrade.ClusterUpgradeStateManagerImpl)
		Expect(stateManager.IsValidationEnabled()).To(Equal(true))
		// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(podRestartNode)).To(Equal(upgrade.UpgradeStateValidationRequired))
	})
	It("UpgradeStateManager should move pod to UpgradeUncordonRequired state "+
		"if it's in ValidationRequired and validation has completed", func() {
		ctx := context.TODO()

		node := NewNode(fmt.Sprintf("node1-%s", id)).
			WithUpgradeState(upgrade.UpgradeStateValidationRequired).
			Create()

		namespace := createNamespace(fmt.Sprintf("namespace-%s", id))
		_ = NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, node.Name).
			WithLabels(map[string]string{"app": "validator"}).
			Create()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateValidationRequired] = []*upgrade.NodeUpgradeState{
			{
				Node:            node,
				DriverPod:       &corev1.Pod{},
				DriverDaemonSet: &appsv1.DaemonSet{},
			},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		stateManager = stateManager.WithValidationEnabled("app=validator").(*upgrade.ClusterUpgradeStateManagerImpl)
		Expect(stateManager.IsValidationEnabled()).To(Equal(true))
		// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(node)).To(Equal(upgrade.UpgradeStateUncordonRequired))
	})
	It("UpgradeStateManager should move pod to UpgradeDone state"+
		"if it's in ValidationRequired, validation has completed, and node was initially Unschedulable", func() {
		ctx := context.TODO()

		node := NewNode(fmt.Sprintf("node1-%s", id)).
			WithUpgradeState(upgrade.UpgradeStateValidationRequired).
			WithAnnotations(map[string]string{upgrade.GetUpgradeInitialStateAnnotationKey(): "true"}).
			Unschedulable(true).
			Create()

		namespace := createNamespace(fmt.Sprintf("namespace-%s", id))
		_ = NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, node.Name).
			WithLabels(map[string]string{"app": "validator"}).
			Create()

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStateValidationRequired] = []*upgrade.NodeUpgradeState{
			{
				Node:            node,
				DriverPod:       &corev1.Pod{},
				DriverDaemonSet: &appsv1.DaemonSet{},
			},
		}

		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
		}

		stateManager = stateManager.WithValidationEnabled("app=validator").(*upgrade.ClusterUpgradeStateManagerImpl)
		Expect(stateManager.IsValidationEnabled()).To(Equal(true))
		// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(node)).To(Equal(upgrade.UpgradeStateDone))
		// unschedulable annotation should be removed
		Expect(isUnschedulableAnnotationPresent(node)).To(Equal(false))
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
