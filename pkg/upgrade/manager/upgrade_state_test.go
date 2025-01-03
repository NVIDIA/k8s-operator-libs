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
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
	upgrade "github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/manager"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/manager/mocks"
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

	Describe("BuildState", func() {
		var namespace *corev1.Namespace

		BeforeEach(func() {
			namespace = createNamespace(fmt.Sprintf("namespace-%s", id))
		})

		It("should not fail when no pods exist", func() {
			upgradeState, err := stateManager.BuildState(ctx, namespace.Name, map[string]string{"foo": "bar"})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(upgradeState.NodeStates)).To(Equal(0))
		})

		It("should process running daemonset pods without error", func() {
			selector := map[string]string{"foo": "bar"}
			node := createNode(fmt.Sprintf("node-%s", id))
			ds := NewDaemonSet(fmt.Sprintf("ds-%s", id), namespace.Name, selector).
				WithDesiredNumberScheduled(1).
				WithLabels(map[string]string{"foo": "bar"}).
				Create()
			_ = NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, node.Name).
				WithLabels(selector).
				WithOwnerReference(v1.OwnerReference{
					APIVersion: "apps/v1",
					Kind:       "DaemonSet",
					Name:       ds.Name,
					UID:        ds.UID,
				}).
				Create()

			upgradeState, err := stateManager.BuildState(ctx, namespace.Name, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(upgradeState.NodeStates)).To(Equal(1))
		})

		It("should not process daemonset pods which have not been scheduled yet", func() {
			selector := map[string]string{"foo": "bar"}
			ds := NewDaemonSet(fmt.Sprintf("ds-%s", id), namespace.Name, selector).
				WithDesiredNumberScheduled(1).
				WithLabels(map[string]string{"foo": "bar"}).
				Create()
			pod := NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, "").
				WithLabels(selector).
				WithOwnerReference(v1.OwnerReference{
					APIVersion: "apps/v1",
					Kind:       "DaemonSet",
					Name:       ds.Name,
					UID:        ds.UID,
				}).
				Create()
			pod.Status.Phase = corev1.PodPending
			err := updatePodStatus(pod)
			Expect(err).To(Succeed())

			upgradeState, err := stateManager.BuildState(ctx, namespace.Name, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(upgradeState.NodeStates)).To(Equal(0))
		})

		It("should process orphaned pods without error", func() {
			selector := map[string]string{"foo": "bar"}
			node := createNode(fmt.Sprintf("node-%s", id))
			_ = NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, node.Name).
				WithLabels(selector).
				Create()

			upgradeState, err := stateManager.BuildState(ctx, namespace.Name, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(upgradeState.NodeStates)).To(Equal(1))
			Expect(upgradeState.NodeStates[""][0].IsOrphanedPod()).To(BeTrue())
		})
	})

	Describe("ApplyState", func() {
		It("UpgradeStateManager should fail on nil currentState", func() {
			Expect(stateManager.ApplyState(ctx, nil, &v1alpha1.DriverUpgradePolicySpec{})).ToNot(Succeed())
		})
		It("UpgradeStateManager should not fail on nil upgradePolicy", func() {
			Expect(stateManager.ApplyState(ctx, &base.ClusterUpgradeState{}, nil)).To(Succeed())
		})
		It("UpgradeStateManager should move up-to-date nodes to Done and outdated nodes to UpgradeRequired states", func() {
			daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
			upToDatePod := &corev1.Pod{
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
			outdatedPod := &corev1.Pod{
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-outadated"}}}

			UnknownToDoneNode := nodeWithUpgradeState("")
			UnknownToUpgradeRequiredNode := nodeWithUpgradeState("")
			DoneToDoneNode := nodeWithUpgradeState(base.UpgradeStateDone)
			DoneToUpgradeRequiredNode := nodeWithUpgradeState(base.UpgradeStateDone)

			clusterState := base.NewClusterUpgradeState()
			unknownNodes := []*base.NodeUpgradeState{
				{Node: UnknownToDoneNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet},
				{Node: UnknownToUpgradeRequiredNode, DriverPod: outdatedPod, DriverDaemonSet: daemonSet},
			}
			doneNodes := []*base.NodeUpgradeState{
				{Node: DoneToDoneNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet},
				{Node: DoneToUpgradeRequiredNode, DriverPod: outdatedPod, DriverDaemonSet: daemonSet},
			}
			clusterState.NodeStates[""] = unknownNodes
			clusterState.NodeStates[base.UpgradeStateDone] = doneNodes

			Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
			Expect(getNodeUpgradeState(UnknownToDoneNode)).To(Equal(base.UpgradeStateDone))
			Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(base.UpgradeStateUpgradeRequired))
			Expect(getNodeUpgradeState(DoneToDoneNode)).To(Equal(base.UpgradeStateDone))
			Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(base.UpgradeStateUpgradeRequired))
		})
		It("UpgradeStateManager should move outdated nodes to UpgradeRequired state and annotate node if unschedulable", func() {
			ctx := context.TODO()

			daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
			upToDatePod := &corev1.Pod{
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
			outdatedPod := &corev1.Pod{
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}

			UnknownToDoneNode := NewNode(fmt.Sprintf("node1-%s", id)).Create()
			UnknownToUpgradeRequiredNode := NewNode(fmt.Sprintf("node2-%s", id)).Unschedulable(true).Create()
			DoneToDoneNode := NewNode(fmt.Sprintf("node3-%s", id)).WithUpgradeState(base.UpgradeStateDone).Create()
			DoneToUpgradeRequiredNode := NewNode(fmt.Sprintf("node4-%s", id)).WithUpgradeState(base.UpgradeStateDone).Unschedulable(true).Create()

			clusterState := base.NewClusterUpgradeState()
			unknownNodes := []*base.NodeUpgradeState{
				{Node: UnknownToDoneNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet},
				{Node: UnknownToUpgradeRequiredNode, DriverPod: outdatedPod, DriverDaemonSet: daemonSet},
			}
			doneNodes := []*base.NodeUpgradeState{
				{Node: DoneToDoneNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet},
				{Node: DoneToUpgradeRequiredNode, DriverPod: outdatedPod, DriverDaemonSet: daemonSet},
			}
			clusterState.NodeStates[""] = unknownNodes
			clusterState.NodeStates[base.UpgradeStateDone] = doneNodes

			provider := base.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
			Expect(getNodeUpgradeState(UnknownToDoneNode)).To(Equal(base.UpgradeStateDone))
			Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(base.UpgradeStateUpgradeRequired))
			Expect(getNodeUpgradeState(DoneToDoneNode)).To(Equal(base.UpgradeStateDone))
			Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(base.UpgradeStateUpgradeRequired))

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

			safeLoadAnnotationKey := base.GetUpgradeDriverWaitForSafeLoadAnnotationKey()
			daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
			upToDatePod := &corev1.Pod{
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}

			waitForSafeLoadNode := NewNode(fmt.Sprintf("node1-%s", id)).
				WithAnnotations(map[string]string{safeLoadAnnotationKey: "true"}).
				Create()
			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateDone] = []*base.NodeUpgradeState{{
				Node: waitForSafeLoadNode, DriverPod: upToDatePod, DriverDaemonSet: daemonSet,
			}}

			provider := base.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
			Expect(getNodeUpgradeState(waitForSafeLoadNode)).To(Equal(base.UpgradeStateUpgradeRequired))
		})
		It("UpgradeStateManager should schedule upgrade on all nodes if maxParallel upgrades is set to 0", func() {
			clusterState := base.NewClusterUpgradeState()
			nodeStates := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
			}

			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = nodeStates

			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
				// Unlimited upgrades
				MaxParallelUpgrades: 0,
			}

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
			stateCount := make(map[string]int)
			for i := range nodeStates {
				state := getNodeUpgradeState(clusterState.NodeStates[base.UpgradeStateUpgradeRequired][i].Node)
				stateCount[state]++
			}
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(0))
			Expect(stateCount[base.UpgradeStateCordonRequired]).To(Equal(len(nodeStates)))
		})
		It("UpgradeStateManager should start upgrade on limited amount of nodes "+
			"if maxParallel upgrades is less than node count", func() {
			const maxParallelUpgrades = 3

			clusterState := base.NewClusterUpgradeState()
			nodeStates := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
			}

			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = nodeStates

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
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(2))
			Expect(stateCount[base.UpgradeStateCordonRequired]).To(Equal(maxParallelUpgrades))
		})
		It("UpgradeStateManager should start additional upgrades if maxParallelUpgrades limit is not reached", func() {
			const maxParallelUpgrades = 4

			upgradeRequiredNodes := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
			}
			cordonRequiredNodes := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
			}
			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = upgradeRequiredNodes
			clusterState.NodeStates[base.UpgradeStateCordonRequired] = cordonRequiredNodes

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
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(1))
			Expect(stateCount[base.UpgradeStateCordonRequired] +
				stateCount[base.UpgradeStateWaitForJobsRequired]).To(Equal(4))
		})
		It("UpgradeStateManager should schedule upgrade all nodes if maxParallel upgrades is set to 0 and maxUnavailable is set to 100%", func() {
			clusterState := base.NewClusterUpgradeState()
			nodeStates := []*base.NodeUpgradeState{
				{Node: NewNode("node1").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node2").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node3").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node4").WithUpgradeState(base.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
				{Node: NewNode("node5").WithUpgradeState(base.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
			}

			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = nodeStates

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
				state := getNodeUpgradeState(clusterState.NodeStates[base.UpgradeStateUpgradeRequired][i].Node)
				stateCount[state]++
			}
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(0))
			Expect(stateCount[base.UpgradeStateCordonRequired]).To(Equal(len(nodeStates)))
		})
		It("UpgradeStateManager should schedule upgrade based on maxUnavailable constraint if maxParallel upgrades is set to 0 and maxUnavailable is set to 50%", func() {
			clusterState := base.NewClusterUpgradeState()
			nodeStates := []*base.NodeUpgradeState{
				{Node: NewNode("node1").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node2").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node3").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node4").WithUpgradeState(base.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
				{Node: NewNode("node5").WithUpgradeState(base.UpgradeStateUpgradeRequired).Unschedulable(true).Node},
			}

			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = nodeStates

			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
				// Unlimited upgrades
				MaxParallelUpgrades: 0,
				MaxUnavailable:      &intstr.IntOrString{Type: intstr.String, StrVal: "50%"},
			}

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
			stateCount := make(map[string]int)
			for i := range nodeStates {
				state := getNodeUpgradeState(clusterState.NodeStates[base.UpgradeStateUpgradeRequired][i].Node)
				stateCount[state]++
			}
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(2))
			Expect(stateCount[base.UpgradeStateCordonRequired]).To(Equal(3))
		})
		It("UpgradeStateManager should schedule upgrade based on 50% maxUnavailable, with some unavailable nodes already upgraded", func() {
			clusterState := base.NewClusterUpgradeState()
			daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
			upToDatePod := &corev1.Pod{
				Status:     corev1.PodStatus{Phase: "Running"},
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}

			upgradeRequiredNodes := []*base.NodeUpgradeState{
				{Node: NewNode("node1").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node2").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
				{Node: NewNode("node3").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
			}
			upgradeDoneNodes := []*base.NodeUpgradeState{
				{
					Node:            NewNode("node4").WithUpgradeState(base.UpgradeStateDone).Unschedulable(true).Node,
					DriverPod:       upToDatePod,
					DriverDaemonSet: daemonSet,
				},
				{
					Node:            NewNode("node5").WithUpgradeState(base.UpgradeStateDone).Unschedulable(true).Node,
					DriverPod:       upToDatePod,
					DriverDaemonSet: daemonSet,
				},
			}
			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = upgradeRequiredNodes
			clusterState.NodeStates[base.UpgradeStateDone] = upgradeDoneNodes

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
						return pod.Labels[base.PodControllerRevisionHashLabelKey]
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
				state := getNodeUpgradeState(clusterState.NodeStates[base.UpgradeStateUpgradeRequired][i].Node)
				stateCount[state]++
			}
			for i := range upgradeDoneNodes {
				state := getNodeUpgradeState(clusterState.NodeStates[base.UpgradeStateDone][i].Node)
				stateCount[state]++
			}
			// check if already upgraded node states are not changed
			Expect(stateCount[base.UpgradeStateDone]).To(Equal(2))
			// expect only single node to move to next state as upgradesUnavailble = maxUnavailable(3) - currentlyUnavailable(2)
			Expect(stateCount[base.UpgradeStateCordonRequired]).To(Equal(1))
			// remaining nodes to be in same original state
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(2))
		})
		It("UpgradeStateManager should start upgrade on limited amount of nodes "+
			"if maxParallel upgrades  and maxUnavailable are less than node count", func() {
			const maxParallelUpgrades = 3

			clusterState := base.NewClusterUpgradeState()
			nodeStates := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
			}

			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = nodeStates

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
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(3))
			// only maxUnavailable nodes should progress to next state
			Expect(stateCount[base.UpgradeStateCordonRequired]).To(Equal(2))
		})
		It("UpgradeStateManager should start additional upgrades if maxParallelUpgrades and maxUnavailable limits are not reached", func() {
			const maxParallelUpgrades = 4

			upgradeRequiredNodes := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
			}
			cordonRequiredNodes := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
			}
			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = upgradeRequiredNodes
			clusterState.NodeStates[base.UpgradeStateCordonRequired] = cordonRequiredNodes

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
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(1))
			Expect(stateCount[base.UpgradeStateCordonRequired] +
				stateCount[base.UpgradeStateWaitForJobsRequired]).To(Equal(4))
		})
		It("UpgradeStateManager should start additional upgrades if maxParallelUpgrades and maxUnavailable limits are not reached", func() {
			const maxParallelUpgrades = 4

			upgradeRequiredNodes := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)},
			}
			cordonRequiredNodes := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateCordonRequired)},
			}
			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = upgradeRequiredNodes
			clusterState.NodeStates[base.UpgradeStateCordonRequired] = cordonRequiredNodes

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
			Expect(stateCount[base.UpgradeStateUpgradeRequired]).To(Equal(1))
			Expect(stateCount[base.UpgradeStateCordonRequired] +
				stateCount[base.UpgradeStateWaitForJobsRequired]).To(Equal(4))
		})
		It("UpgradeStateManager should skip pod deletion if no filter is provided to PodManager at contruction", func() {
			ctx := context.TODO()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateWaitForJobsRequired] = []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateWaitForJobsRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateWaitForJobsRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateWaitForJobsRequired)},
			}

			policyWithNoDrainSpec := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			Expect(stateManager.ApplyState(ctx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
			for _, state := range clusterState.NodeStates[base.UpgradeStateWaitForJobsRequired] {
				Expect(getNodeUpgradeState(state.Node)).To(Equal(base.UpgradeStateDrainRequired))
			}
		})
		It("UpgradeStateManager should not skip pod deletion if a filter is provided to PodManager at contruction", func() {
			ctx := context.TODO()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateWaitForJobsRequired] = []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateWaitForJobsRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateWaitForJobsRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateWaitForJobsRequired)},
			}

			policyWithNoDrainSpec := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			filter := func(pod corev1.Pod) bool { return false }
			commonStateManager := stateManager.CommonUpgradeManagerImpl.WithPodDeletionEnabled(filter)
			Expect(commonStateManager.IsPodDeletionEnabled()).To(Equal(true))

			Expect(stateManager.ApplyState(ctx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
			for _, state := range clusterState.NodeStates[base.UpgradeStateWaitForJobsRequired] {
				Expect(getNodeUpgradeState(state.Node)).To(Equal(base.UpgradeStatePodDeletionRequired))
			}

		})
		It("UpgradeStateManager should not attempt to delete pods if pod deletion is disabled", func() {
			ctx := context.TODO()

			clusterState := base.NewClusterUpgradeState()
			nodes := []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStatePodDeletionRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStatePodDeletionRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStatePodDeletionRequired)},
			}

			clusterState.NodeStates[base.UpgradeStatePodDeletionRequired] = nodes

			policyWithNoPodDeletionSpec := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			podEvictionCalled := false

			podManagerMock := mocks.PodManager{}
			podManagerMock.
				On("SchedulePodEviction", mock.Anything, mock.Anything).
				Return(func(ctx context.Context, config *base.PodManagerConfig) error {
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
				Expect(getNodeUpgradeState(state.Node)).To(Equal(base.UpgradeStateDrainRequired))
			}
		})
		It("UpgradeStateManager should skip drain if it's disabled by policy", func() {
			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateDrainRequired] = []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
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
			for _, state := range clusterState.NodeStates[base.UpgradeStateDrainRequired] {
				Expect(getNodeUpgradeState(state.Node)).To(Equal(base.UpgradeStatePodRestartRequired))
			}

			clusterState.NodeStates[base.UpgradeStateDrainRequired] = []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
			}
			Expect(stateManager.ApplyState(ctx, &clusterState, policyWithDisabledDrain)).To(Succeed())
			for _, state := range clusterState.NodeStates[base.UpgradeStateDrainRequired] {
				Expect(getNodeUpgradeState(state.Node)).To(Equal(base.UpgradeStatePodRestartRequired))
			}
		})
		It("UpgradeStateManager should schedule drain for UpgradeStateDrainRequired nodes and pass drain config", func() {
			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateDrainRequired] = []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
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
				Return(func(ctx context.Context, config *base.DrainConfiguration) error {
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
			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateDrainRequired] = []*base.NodeUpgradeState{
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(base.UpgradeStateDrainRequired)},
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
				Return(func(ctx context.Context, config *base.DrainConfiguration) error {
					return errors.New("drain failed")
				})
			stateManager.DrainManager = &drainManagerMock

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).ToNot(Succeed())
		})
		It("UpgradeStateManager should not restart pod if it's up to date or already terminating", func() {
			daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
			upToDatePod := &corev1.Pod{
				Status:     corev1.PodStatus{Phase: "Running"},
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
			outdatedRunningPod := &corev1.Pod{
				Status:     corev1.PodStatus{Phase: "Running"},
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}
			outdatedTerminatingPod := &corev1.Pod{
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}
			now := v1.Now()
			outdatedTerminatingPod.ObjectMeta.DeletionTimestamp = &now

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStatePodRestartRequired] = []*base.NodeUpgradeState{
				{
					Node:            nodeWithUpgradeState(base.UpgradeStatePodRestartRequired),
					DriverPod:       upToDatePod,
					DriverDaemonSet: daemonSet,
				},
				{
					Node:            nodeWithUpgradeState(base.UpgradeStatePodRestartRequired),
					DriverPod:       outdatedRunningPod,
					DriverDaemonSet: daemonSet,
				},
				{
					Node:            nodeWithUpgradeState(base.UpgradeStatePodRestartRequired),
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
						return pod.Labels[base.PodControllerRevisionHashLabelKey]
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
			safeLoadAnnotation := base.GetUpgradeDriverWaitForSafeLoadAnnotationKey()
			daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}

			upToDatePod := &corev1.Pod{
				Status:     corev1.PodStatus{Phase: "Running"},
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}

			waitForSafeLoadNode := NewNode(fmt.Sprintf("node1-%s", id)).
				WithUpgradeState(base.UpgradeStatePodRestartRequired).
				WithAnnotations(map[string]string{safeLoadAnnotation: "true"}).
				Create()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStatePodRestartRequired] = []*base.NodeUpgradeState{
				{
					Node:            waitForSafeLoadNode,
					DriverPod:       upToDatePod,
					DriverDaemonSet: daemonSet,
				},
			}

			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}
			provider := base.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
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
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
			podRestartNode := NewNode("pod-restart-node").WithUpgradeState(base.UpgradeStatePodRestartRequired).Create()
			upgradeFailedNode := NewNode("upgrade-failed-node").WithUpgradeState(base.UpgradeStateFailed).Create()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStatePodRestartRequired] = []*base.NodeUpgradeState{
				{
					Node:            podRestartNode,
					DriverPod:       pod,
					DriverDaemonSet: daemonSet,
				},
			}
			clusterState.NodeStates[base.UpgradeStateFailed] = []*base.NodeUpgradeState{
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
			Expect(getNodeUpgradeState(podRestartNode)).To(Equal(base.UpgradeStateUncordonRequired))
			Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(base.UpgradeStateUncordonRequired))
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
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}}}
			podRestartNode := NewNode("pod-restart-node-unschedulable").
				WithUpgradeState(base.UpgradeStatePodRestartRequired).
				WithAnnotations(map[string]string{base.GetUpgradeInitialStateAnnotationKey(): "true"}).
				Unschedulable(true).
				Create()
			upgradeFailedNode := NewNode("upgrade-failed-node-unschedulable").
				WithUpgradeState(base.UpgradeStateFailed).
				WithAnnotations(map[string]string{base.GetUpgradeInitialStateAnnotationKey(): "true"}).
				Unschedulable(true).
				Create()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStatePodRestartRequired] = []*base.NodeUpgradeState{
				{
					Node:            podRestartNode,
					DriverPod:       pod,
					DriverDaemonSet: daemonSet,
				},
			}
			clusterState.NodeStates[base.UpgradeStateFailed] = []*base.NodeUpgradeState{
				{
					Node:            upgradeFailedNode,
					DriverPod:       pod,
					DriverDaemonSet: daemonSet,
				},
			}

			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			provider := base.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(podRestartNode)).To(Equal(base.UpgradeStateDone))
			Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(base.UpgradeStateDone))
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
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}},
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
					WithUpgradeState(base.UpgradeStatePodRestartRequired).
					Create()
			}

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStatePodRestartRequired] = []*base.NodeUpgradeState{
				{Node: nodes[0], DriverPod: pod1, DriverDaemonSet: daemonSet},
				{Node: nodes[1], DriverPod: pod2, DriverDaemonSet: daemonSet},
				{Node: nodes[2], DriverPod: pod3, DriverDaemonSet: daemonSet},
				{Node: nodes[3], DriverPod: pod4, DriverDaemonSet: daemonSet},
			}

			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(nodes[0])).To(Equal(base.UpgradeStatePodRestartRequired))
			Expect(getNodeUpgradeState(nodes[1])).To(Equal(base.UpgradeStatePodRestartRequired))
			Expect(getNodeUpgradeState(nodes[2])).To(Equal(base.UpgradeStateFailed))
			Expect(getNodeUpgradeState(nodes[3])).To(Equal(base.UpgradeStateFailed))
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
				ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-12345"}},
			}
			podRestartNode := NewNode(fmt.Sprintf("node1-%s", id)).
				WithUpgradeState(base.UpgradeStatePodRestartRequired).
				Create()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStatePodRestartRequired] = []*base.NodeUpgradeState{
				{
					Node:            podRestartNode,
					DriverPod:       pod,
					DriverDaemonSet: daemonSet,
				},
			}
			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}
			commonStateManager := stateManager.CommonUpgradeManagerImpl.WithValidationEnabled("app=validator")
			Expect(commonStateManager.IsValidationEnabled()).To(Equal(true))
			// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
			provider := base.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(podRestartNode)).To(Equal(base.UpgradeStateValidationRequired))
		})
		It("UpgradeStateManager should move pod to UpgradeUncordonRequired state "+
			"if it's in ValidationRequired and validation has completed", func() {
			ctx := context.TODO()

			node := NewNode(fmt.Sprintf("node1-%s", id)).
				WithUpgradeState(base.UpgradeStateValidationRequired).
				Create()

			namespace := createNamespace(fmt.Sprintf("namespace-%s", id))
			_ = NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"app": "validator"}).
				Create()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateValidationRequired] = []*base.NodeUpgradeState{
				{
					Node:            node,
					DriverPod:       &corev1.Pod{},
					DriverDaemonSet: &appsv1.DaemonSet{},
				},
			}

			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			commonStateManager := stateManager.CommonUpgradeManagerImpl.WithValidationEnabled("app=validator")
			Expect(commonStateManager.IsValidationEnabled()).To(Equal(true))
			// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
			provider := base.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(node)).To(Equal(base.UpgradeStateUncordonRequired))
		})
		It("UpgradeStateManager should move pod to UpgradeDone state"+
			"if it's in ValidationRequired, validation has completed, and node was initially Unschedulable", func() {
			ctx := context.TODO()

			node := NewNode(fmt.Sprintf("node1-%s", id)).
				WithUpgradeState(base.UpgradeStateValidationRequired).
				WithAnnotations(map[string]string{base.GetUpgradeInitialStateAnnotationKey(): "true"}).
				Unschedulable(true).
				Create()

			namespace := createNamespace(fmt.Sprintf("namespace-%s", id))
			_ = NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, node.Name).
				WithLabels(map[string]string{"app": "validator"}).
				Create()

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateValidationRequired] = []*base.NodeUpgradeState{
				{
					Node:            node,
					DriverPod:       &corev1.Pod{},
					DriverDaemonSet: &appsv1.DaemonSet{},
				},
			}

			policy := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			commonStateManager := stateManager.CommonUpgradeManagerImpl.WithValidationEnabled("app=validator")
			Expect(commonStateManager.IsValidationEnabled()).To(Equal(true))
			// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
			provider := base.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(node)).To(Equal(base.UpgradeStateDone))
			// unschedulable annotation should be removed
			Expect(isUnschedulableAnnotationPresent(node)).To(Equal(false))
		})
		It("UpgradeStateManager should uncordon UncordonRequired pod and finish upgrade", func() {
			node := nodeWithUpgradeState(base.UpgradeStateUncordonRequired)

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateUncordonRequired] = []*base.NodeUpgradeState{
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
			Expect(getNodeUpgradeState(node)).To(Equal(base.UpgradeStateDone))
		})
		It("UpgradeStateManager should fail if cordonManager fails", func() {
			node := nodeWithUpgradeState(base.UpgradeStateUncordonRequired)

			clusterState := base.NewClusterUpgradeState()
			clusterState.NodeStates[base.UpgradeStateUncordonRequired] = []*base.NodeUpgradeState{
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
			Expect(getNodeUpgradeState(node)).ToNot(Equal(base.UpgradeStateDone))
		})
	})
	It("UpgradeStateManager should not move outdated node to UpgradeRequired states with orphaned pod", func() {
		orphanedPod := &corev1.Pod{}

		UnknownToUpgradeDoneNode := nodeWithUpgradeState("")
		DoneToUpgradeDoneNode := nodeWithUpgradeState(base.UpgradeStateDone)

		clusterState := base.NewClusterUpgradeState()
		unknownNodes := []*base.NodeUpgradeState{
			{Node: UnknownToUpgradeDoneNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		doneNodes := []*base.NodeUpgradeState{
			{Node: DoneToUpgradeDoneNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		clusterState.NodeStates[""] = unknownNodes
		clusterState.NodeStates[base.UpgradeStateDone] = doneNodes

		Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToUpgradeDoneNode)).To(Equal(base.UpgradeStateDone))
		Expect(getNodeUpgradeState(DoneToUpgradeDoneNode)).To(Equal(base.UpgradeStateDone))
	})
	It("UpgradeStateManager should move outdated node to UpgradeRequired states with orphaned pod if upgrade-requested", func() {
		orphanedPod := &corev1.Pod{}

		UnknownToUpgradeRequiredNode := nodeWithUpgradeState("")
		UnknownToUpgradeRequiredNode.Annotations[base.GetUpgradeRequestedAnnotationKey()] = "true"
		DoneToUpgradeRequiredNode := nodeWithUpgradeState(base.UpgradeStateDone)
		DoneToUpgradeRequiredNode.Annotations[base.GetUpgradeRequestedAnnotationKey()] = "true"

		clusterState := base.NewClusterUpgradeState()
		unknownNodes := []*base.NodeUpgradeState{
			{Node: UnknownToUpgradeRequiredNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		doneNodes := []*base.NodeUpgradeState{
			{Node: DoneToUpgradeRequiredNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		clusterState.NodeStates[""] = unknownNodes
		clusterState.NodeStates[base.UpgradeStateDone] = doneNodes

		Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(base.UpgradeStateUpgradeRequired))
		Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(base.UpgradeStateUpgradeRequired))
	})
	It("UpgradeStateManager should move upgrade required node to CordonRequired states with orphaned pod and remove upgrade-requested annotation", func() {
		orphanedPod := &corev1.Pod{}

		UpgradeRequiredToCordonNodes := nodeWithUpgradeState(base.UpgradeStateUpgradeRequired)
		UpgradeRequiredToCordonNodes.Annotations[base.GetUpgradeRequestedAnnotationKey()] = "true"

		clusterState := base.NewClusterUpgradeState()
		upgradeRequiredNodes := []*base.NodeUpgradeState{
			{Node: UpgradeRequiredToCordonNodes, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		clusterState.NodeStates[base.UpgradeStateUpgradeRequired] = upgradeRequiredNodes

		Expect(stateManager.ApplyState(ctx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UpgradeRequiredToCordonNodes)).To(Equal(base.UpgradeStateCordonRequired))
		Expect(UpgradeRequiredToCordonNodes.Annotations[base.GetUpgradeRequestedAnnotationKey()]).To(Equal(""))
	})
	It("UpgradeStateManager should restart pod if it is Orphaned", func() {
		orphanedPod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{base.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}

		clusterState := base.NewClusterUpgradeState()
		clusterState.NodeStates[base.UpgradeStatePodRestartRequired] = []*base.NodeUpgradeState{
			{
				Node:            nodeWithUpgradeState(base.UpgradeStatePodRestartRequired),
				DriverPod:       orphanedPod,
				DriverDaemonSet: nil,
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
				Expect(podsToDelete[0]).To(Equal(orphanedPod))
				return nil
			})
		stateManager.PodManager = &podManagerMock

		Expect(stateManager.ApplyState(ctx, &clusterState, policy)).To(Succeed())
	})
	It("UpgradeStateManager should not move to UncordonRequired state "+
		"if it's in UpgradeFailed, and Orphaned Pod", func() {
		daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:             "Running",
				ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
			},
		}
		upgradeFailedNode := NewNode("upgrade-failed-node").WithUpgradeState(base.UpgradeStateFailed).Create()

		clusterState := base.NewClusterUpgradeState()
		clusterState.NodeStates[base.UpgradeStateFailed] = []*base.NodeUpgradeState{
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
		Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(base.UpgradeStateFailed))
	})

})

func nodeWithUpgradeState(state string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: v1.ObjectMeta{
			Labels:      map[string]string{base.GetUpgradeStateLabelKey(): state},
			Annotations: map[string]string{},
		},
	}
}
