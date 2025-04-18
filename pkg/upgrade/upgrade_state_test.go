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
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	v1alpha1 "github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	upgrade "github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/mocks"
)

var (
	stateManager          *upgrade.ClusterUpgradeStateManagerImpl
	stateManagerInterface upgrade.ClusterUpgradeStateManager
	opts                  upgrade.StateOptions
)

var _ = Describe("UpgradeStateManager tests", func() {
	var id string

	BeforeEach(func() {
		var err error
		id = randSeq(5)
		opts := upgrade.StateOptions{}
		// Create new ClusterUpgradeStateManagerImpl using mocked managers initialized in BeforeSuite()
		stateManagerInterface, err = upgrade.NewClusterUpgradeStateManager(log, k8sConfig,
			eventRecorder, opts)
		Expect(err).NotTo(HaveOccurred())

		stateManager, _ = stateManagerInterface.(*upgrade.ClusterUpgradeStateManagerImpl)
		stateManager.NodeUpgradeStateProvider = &nodeUpgradeStateProvider
		stateManager.DrainManager = &drainManager
		stateManager.CordonManager = &cordonManager
		stateManager.PodManager = &podManager
		stateManager.ValidationManager = &validationManager
	})

	AfterEach(func() {
		By("Cleanup NodeMaintenance resources")
		Eventually(func() bool {
			nodes := &corev1.NodeList{}
			err := k8sClient.List(testCtx, nodes)
			if err != nil && k8serrors.IsNotFound(err) {
				return true
			}
			Expect(err).NotTo(HaveOccurred())
			if len(nodes.Items) == 0 {
				return true
			}
			Expect(err).NotTo(HaveOccurred())
			for _, item := range nodes.Items {
				err = k8sClient.Delete(testCtx, &item)
				if err != nil && k8serrors.IsNotFound(err) {
					err = nil
				}
				Expect(err).NotTo(HaveOccurred())
			}
			return false
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		Eventually(func() bool {
			nms := &maintenancev1alpha1.NodeMaintenanceList{}
			err := k8sClient.List(testCtx, nms)
			if err != nil && k8serrors.IsNotFound(err) {
				return true
			}
			Expect(err).NotTo(HaveOccurred())
			if len(nms.Items) == 0 {
				return true
			}
			for _, item := range nms.Items {
				err = removeFinalizersOrDelete(testCtx, &item)
				Expect(err).NotTo(HaveOccurred())
			}
			return false
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())
	})

	Describe("BuildState", func() {
		var namespace *corev1.Namespace

		BeforeEach(func() {
			namespace = createNamespace(fmt.Sprintf("namespace-%s", id))
		})

		It("should not fail when no pods exist", func() {
			upgradeState, err := stateManagerInterface.BuildState(testCtx, namespace.Name, map[string]string{"foo": "bar"})
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

			upgradeState, err := stateManagerInterface.BuildState(testCtx, namespace.Name, selector)
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

			upgradeState, err := stateManagerInterface.BuildState(testCtx, namespace.Name, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(upgradeState.NodeStates)).To(Equal(0))
		})

		It("should process orphaned pods without error", func() {
			selector := map[string]string{"foo": "bar"}
			node := createNode(fmt.Sprintf("node-%s", id))
			_ = NewPod(fmt.Sprintf("pod-%s", id), namespace.Name, node.Name).
				WithLabels(selector).
				Create()

			upgradeState, err := stateManagerInterface.BuildState(testCtx, namespace.Name, selector)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(upgradeState.NodeStates)).To(Equal(1))
			Expect(upgradeState.NodeStates[""][0].IsOrphanedPod()).To(BeTrue())
		})
	})

	Describe("ApplyState", func() {

		It("UpgradeStateManager should fail on nil currentState", func() {
			Expect(stateManagerInterface.ApplyState(testCtx, nil, &v1alpha1.DriverUpgradePolicySpec{})).ToNot(Succeed())
		})
		It("UpgradeStateManager should not fail on nil upgradePolicy", func() {
			Expect(stateManagerInterface.ApplyState(testCtx, &upgrade.ClusterUpgradeState{}, nil)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
			Expect(getNodeUpgradeState(UnknownToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
			Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
			Expect(getNodeUpgradeState(DoneToDoneNode)).To(Equal(upgrade.UpgradeStateDone))
			Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
		})
		It("UpgradeStateManager should move outdated nodes to UpgradeRequired state and annotate node if unschedulable", func() {
			testCtx := context.TODO()

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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
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
			testCtx := context.TODO()

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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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
				Return(func(testCtx context.Context, podsToDelete []*corev1.Pod) error {
					Expect(podsToDelete).To(HaveLen(0))
					return nil
				})
			podManagerMock.
				On("GetPodControllerRevisionHash", mock.Anything).
				Return(
					func(pod *corev1.Pod) string {
						return pod.Labels[upgrade.PodControllerRevisionHashLabelKey]
					},
					func(pod *corev1.Pod) error {
						return nil
					},
				)
			podManagerMock.
				On("GetDaemonsetControllerRevisionHash", mock.Anything, mock.Anything, mock.Anything).
				Return("test-hash-12345", nil)
			stateManager.PodManager = &podManagerMock

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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
			testCtx := context.TODO()

			clusterState := upgrade.NewClusterUpgradeState()
			clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] = []*upgrade.NodeUpgradeState{
				{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
				{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
				{Node: nodeWithUpgradeState(upgrade.UpgradeStateWaitForJobsRequired)},
			}

			policyWithNoDrainSpec := &v1alpha1.DriverUpgradePolicySpec{
				AutoUpgrade: true,
			}

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
			for _, state := range clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] {
				Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStateDrainRequired))
			}
		})
		It("UpgradeStateManager should not skip pod deletion if a filter is provided to PodManager at contruction", func() {
			testCtx := context.TODO()

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
			commonStateManager := stateManager.WithPodDeletionEnabled(filter)
			Expect(commonStateManager.IsPodDeletionEnabled()).To(Equal(true))

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
			for _, state := range clusterState.NodeStates[upgrade.UpgradeStateWaitForJobsRequired] {
				Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStatePodDeletionRequired))
			}

		})
		It("UpgradeStateManager should not attempt to delete pods if pod deletion is disabled", func() {
			testCtx := context.TODO()

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
				Return(func(testCtx context.Context, config *upgrade.PodManagerConfig) error {
					podEvictionCalled = true
					return nil
				}).
				On("SchedulePodsRestart", mock.Anything, mock.Anything).
				Return(func(testCtx context.Context, pods []*corev1.Pod) error {
					return nil
				})
			stateManager.PodManager = &podManagerMock

			Eventually(podEvictionCalled).ShouldNot(Equal(true))

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policyWithNoPodDeletionSpec)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policyWithNoDrainSpec)).To(Succeed())
			for _, state := range clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] {
				Expect(getNodeUpgradeState(state.Node)).To(Equal(upgrade.UpgradeStatePodRestartRequired))
			}

			clusterState.NodeStates[upgrade.UpgradeStateDrainRequired] = []*upgrade.NodeUpgradeState{
				{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
				{Node: nodeWithUpgradeState(upgrade.UpgradeStateDrainRequired)},
			}
			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policyWithDisabledDrain)).To(Succeed())
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
				Return(func(testCtx context.Context, config *upgrade.DrainConfiguration) error {
					Expect(config.Spec).To(Equal(&expectedDrainSpec))
					Expect(config.Nodes).To(HaveLen(3))
					return nil
				})
			stateManager.DrainManager = &drainManagerMock

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &policy)).To(Succeed())

			policy.DrainSpec.PodSelector = "test-label=test-value"
			expectedDrainSpec.PodSelector = policy.DrainSpec.PodSelector
			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &policy)).To(Succeed())
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
				Return(func(testCtx context.Context, config *upgrade.DrainConfiguration) error {
					return errors.New("drain failed")
				})
			stateManager.DrainManager = &drainManagerMock

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).ToNot(Succeed())
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
				Return(func(testCtx context.Context, podsToDelete []*corev1.Pod) error {
					Expect(podsToDelete).To(HaveLen(1))
					Expect(podsToDelete[0]).To(Equal(outdatedRunningPod))
					return nil
				})
			podManagerMock.
				On("GetPodControllerRevisionHash", mock.Anything).
				Return(
					func(pod *corev1.Pod) string {
						return pod.Labels[upgrade.PodControllerRevisionHashLabelKey]
					},
					func(pod *corev1.Pod) error {
						return nil
					},
				)
			podManagerMock.
				On("GetDaemonsetControllerRevisionHash", mock.Anything, mock.Anything, mock.Anything).
				Return("test-hash-12345", nil)
			stateManager.PodManager = &podManagerMock

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: waitForSafeLoadNode.Name}, waitForSafeLoadNode)).
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(podRestartNode)).To(Equal(upgrade.UpgradeStateUncordonRequired))
			Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(upgrade.UpgradeStateUncordonRequired))
		})
		It("UpgradeStateManager should move pod to UpgradeDone state "+
			"if it's in PodRestart or UpgradeFailed, driver pod is up-to-date and ready, and node was initially Unschedulable", func() {
			testCtx := context.TODO()

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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(nodes[0])).To(Equal(upgrade.UpgradeStatePodRestartRequired))
			Expect(getNodeUpgradeState(nodes[1])).To(Equal(upgrade.UpgradeStatePodRestartRequired))
			Expect(getNodeUpgradeState(nodes[2])).To(Equal(upgrade.UpgradeStateFailed))
			Expect(getNodeUpgradeState(nodes[3])).To(Equal(upgrade.UpgradeStateFailed))
		})
		It("UpgradeStateManager should move pod to UpgradeValidationRequired state "+
			"if it's in PodRestart, driver pod is up-to-date and ready, and validation is enabled", func() {
			testCtx := context.TODO()

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
			commonStateManager := stateManager.WithValidationEnabled("app=validator")
			Expect(commonStateManager.IsValidationEnabled()).To(Equal(true))
			// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(podRestartNode)).To(Equal(upgrade.UpgradeStateValidationRequired))
		})
		It("UpgradeStateManager should move pod to UpgradeUncordonRequired state "+
			"if it's in ValidationRequired and validation has completed", func() {
			testCtx := context.TODO()

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

			commonStateManager := stateManager.WithValidationEnabled("app=validator")
			Expect(commonStateManager.IsValidationEnabled()).To(Equal(true))
			// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
			Expect(getNodeUpgradeState(node)).To(Equal(upgrade.UpgradeStateUncordonRequired))
		})
		It("UpgradeStateManager should move pod to UpgradeDone state"+
			"if it's in ValidationRequired, validation has completed, and node was initially Unschedulable", func() {
			testCtx := context.TODO()

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

			commonStateManager := stateManager.WithValidationEnabled("app=validator")
			Expect(commonStateManager.IsValidationEnabled()).To(Equal(true))
			// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
			provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
			stateManager.NodeUpgradeStateProvider = provider

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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
				Return(func(testCtx context.Context, node *corev1.Node) error {
					Expect(node).To(Equal(node))
					return nil
				})
			stateManager.CordonManager = &cordonManagerMock

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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
				Return(func(testCtx context.Context, node *corev1.Node) error {
					return errors.New("cordonManagerFailed")
				})
			stateManager.CordonManager = &cordonManagerMock

			Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).ToNot(Succeed())
			Expect(getNodeUpgradeState(node)).ToNot(Equal(upgrade.UpgradeStateDone))
		})
	})
	It("UpgradeStateManager should not move outdated node to UpgradeRequired states with orphaned pod", func() {
		orphanedPod := &corev1.Pod{}

		UnknownToUpgradeDoneNode := nodeWithUpgradeState("")
		DoneToUpgradeDoneNode := nodeWithUpgradeState(upgrade.UpgradeStateDone)

		clusterState := upgrade.NewClusterUpgradeState()
		unknownNodes := []*upgrade.NodeUpgradeState{
			{Node: UnknownToUpgradeDoneNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		doneNodes := []*upgrade.NodeUpgradeState{
			{Node: DoneToUpgradeDoneNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		clusterState.NodeStates[""] = unknownNodes
		clusterState.NodeStates[upgrade.UpgradeStateDone] = doneNodes

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToUpgradeDoneNode)).To(Equal(upgrade.UpgradeStateDone))
		Expect(getNodeUpgradeState(DoneToUpgradeDoneNode)).To(Equal(upgrade.UpgradeStateDone))
	})
	It("UpgradeStateManager should move outdated node to UpgradeRequired states with orphaned pod if upgrade-requested", func() {
		orphanedPod := &corev1.Pod{}

		UnknownToUpgradeRequiredNode := nodeWithUpgradeState("")
		UnknownToUpgradeRequiredNode.Annotations[upgrade.GetUpgradeRequestedAnnotationKey()] = "true"
		DoneToUpgradeRequiredNode := nodeWithUpgradeState(upgrade.UpgradeStateDone)
		DoneToUpgradeRequiredNode.Annotations[upgrade.GetUpgradeRequestedAnnotationKey()] = "true"

		clusterState := upgrade.NewClusterUpgradeState()
		unknownNodes := []*upgrade.NodeUpgradeState{
			{Node: UnknownToUpgradeRequiredNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		doneNodes := []*upgrade.NodeUpgradeState{
			{Node: DoneToUpgradeRequiredNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		clusterState.NodeStates[""] = unknownNodes
		clusterState.NodeStates[upgrade.UpgradeStateDone] = doneNodes

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
		Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
	})
	It("UpgradeStateManager should move upgrade required node to CordonRequired states with orphaned pod and remove upgrade-requested annotation", func() {
		orphanedPod := &corev1.Pod{}

		UpgradeRequiredToCordonNodes := nodeWithUpgradeState(upgrade.UpgradeStateUpgradeRequired)
		UpgradeRequiredToCordonNodes.Annotations[upgrade.GetUpgradeRequestedAnnotationKey()] = "true"

		clusterState := upgrade.NewClusterUpgradeState()
		upgradeRequiredNodes := []*upgrade.NodeUpgradeState{
			{Node: UpgradeRequiredToCordonNodes, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] = upgradeRequiredNodes

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UpgradeRequiredToCordonNodes)).To(Equal(upgrade.UpgradeStateCordonRequired))
		Expect(UpgradeRequiredToCordonNodes.Annotations[upgrade.GetUpgradeRequestedAnnotationKey()]).To(Equal(""))
	})
	It("UpgradeStateManager should restart pod if it is Orphaned", func() {
		orphanedPod := &corev1.Pod{
			Status:     corev1.PodStatus{Phase: "Running"},
			ObjectMeta: v1.ObjectMeta{Labels: map[string]string{upgrade.PodControllerRevisionHashLabelKey: "test-hash-outdated"}}}

		clusterState := upgrade.NewClusterUpgradeState()
		clusterState.NodeStates[upgrade.UpgradeStatePodRestartRequired] = []*upgrade.NodeUpgradeState{
			{
				Node:            nodeWithUpgradeState(upgrade.UpgradeStatePodRestartRequired),
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
			Return(func(testCtx context.Context, podsToDelete []*corev1.Pod) error {
				Expect(podsToDelete).To(HaveLen(1))
				Expect(podsToDelete[0]).To(Equal(orphanedPod))
				return nil
			})
		stateManager.PodManager = &podManagerMock

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
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
		upgradeFailedNode := NewNode("upgrade-failed-node").WithUpgradeState(upgrade.UpgradeStateFailed).Create()

		clusterState := upgrade.NewClusterUpgradeState()
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

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(upgradeFailedNode)).To(Equal(upgrade.UpgradeStateFailed))
	})

	It("UpgradeStateManager should move to 'node-maintenance-required' while using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

		clusterState := withClusterUpgradeState(3, upgrade.UpgradeStateUpgradeRequired, namespace, nil, false)
		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: true,
			},
		}
		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())

		By("verify node requestor-mode-annotation")
		Eventually(func() bool {
			for _, nodeState := range clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired] {
				node := corev1.Node{}
				nodeKey := client.ObjectKey{
					Name: nodeState.Node.Name,
				}
				if err := k8sClient.Get(testCtx, nodeKey, &node); err != nil {
					if _, ok := node.Annotations[upgrade.GetUpgradeRequestorModeAnnotationKey()]; !ok {
						return false
					}
				}
				Expect(node.Annotations[upgrade.GetUpgradeRequestorModeAnnotationKey()]).To(Equal("true"))
			}
			return true
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		By("verify generated node-maintenance obj(s)")
		nms := &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(testCtx, nms)
			return len(nms.Items) == len(clusterState.NodeStates[upgrade.UpgradeStateUpgradeRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		By("set node-maintenance(s) finalizer to mimic maintenance-operator obj deletion ownership")
		for _, item := range nms.Items {
			nm := &maintenancev1alpha1.NodeMaintenance{}
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: item.Name, Namespace: namespace}, nm)
			Expect(err).NotTo(HaveOccurred())

			nm.Finalizers = append(nm.Finalizers, maintenancev1alpha1.MaintenanceFinalizerName)
			err = k8sClient.Update(testCtx, nm)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				nm := &maintenancev1alpha1.NodeMaintenance{}
				err := k8sClient.Get(testCtx, client.ObjectKey{Name: item.Name, Namespace: namespace}, nm)
				if err != nil {
					return err
				}
				if len(nm.Finalizers) == 0 {
					return fmt.Errorf("missing status condition")
				}
				return nil
			}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
		}

		By("verify node is in 'node-maintennace-required' state")
		nodes := &corev1.NodeList{}
		Eventually(func() error {
			k8sClient.List(testCtx, nodes)
			for _, node := range nodes.Items {
				if getNodeUpgradeState(&node) != upgrade.UpgradeStateNodeMaintenanceRequired {
					return fmt.Errorf("missing status condition")
				}
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
	})

	It("UpgradeStateManager should move to 'post-maintenance-required' while using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

		clusterState := withClusterUpgradeState(1, upgrade.UpgradeStateNodeMaintenanceRequired, namespace,
			map[string]string{upgrade.GetUpgradeRequestedAnnotationKey(): "true"}, true)
		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: true,
			},
		}

		// eventually wait for node to be created
		node := &corev1.Node{}
		Eventually(func() error {
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-1", Namespace: namespace}, node)
			if err != nil {
				return err
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())

		nmObj := &maintenancev1alpha1.NodeMaintenance{}
		err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-1", Namespace: namespace}, nmObj)
		Expect(err).NotTo(HaveOccurred())
		By("set node-maintenance(s) status to mimic maintenance-operator 'Ready' condition flow")
		status := maintenancev1alpha1.NodeMaintenanceStatus{
			Conditions: []v1.Condition{{
				Type:               maintenancev1alpha1.ConditionTypeReady,
				Status:             v1.ConditionTrue,
				Reason:             maintenancev1alpha1.ConditionReasonReady,
				Message:            "Maintenance completed successfully",
				LastTransitionTime: v1.NewTime(time.Now()),
			}},
		}
		nmObj.Status = status
		err = k8sClient.Status().Update(testCtx, nmObj)
		Expect(err).NotTo(HaveOccurred())

		nm := &maintenancev1alpha1.NodeMaintenance{}
		Eventually(func() error {
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-1", Namespace: namespace}, nm)
			if err != nil {
				return err
			}
			if len(nm.Status.Conditions) == 0 {
				return fmt.Errorf("missing status condition. '%v'", nm.DeepCopy().Status)
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
		clusterState.NodeStates[upgrade.UpgradeStateNodeMaintenanceRequired][0].NodeMaintenance = nm

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())

		By("verify node is in post-maintenace-required' state")
		node = &corev1.Node{}
		Eventually(func() error {
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-1", Namespace: namespace}, node)
			if err != nil {
				return err
			}
			if getNodeUpgradeState(node) != upgrade.UpgradeStatePodRestartRequired {
				return fmt.Errorf("missing status condition")
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())

	})

	It("UpgradeStateManager should move node to 'upgrade-required' in case nodeMaintenance obj is missing "+
		"while using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

		clusterState := withClusterUpgradeState(1, upgrade.UpgradeStateNodeMaintenanceRequired, namespace,
			map[string]string{upgrade.GetUpgradeRequestedAnnotationKey(): "true"}, true)
		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: true,
			},
		}

		// eventually wait for node to be created
		node := &corev1.Node{}
		Eventually(func() error {
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-1", Namespace: namespace}, node)
			if err != nil {
				return err
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())

		clusterState.NodeStates[upgrade.UpgradeStateNodeMaintenanceRequired][0].NodeMaintenance = nil
		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())

		By("verify node is in 'upgrade-required' state")
		node = &corev1.Node{}
		Eventually(func() error {
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-1", Namespace: namespace}, node)
			if err != nil {
				return err
			}
			if getNodeUpgradeState(node) != upgrade.UpgradeStateUpgradeRequired {
				return fmt.Errorf("missing status condition")
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())

	})

	It("UpgradeStateManager continue inplace upgrade logic, move to 'wait-for-jobs-required' "+
		"while using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

		clusterState := withClusterUpgradeState(3, upgrade.UpgradeStateCordonRequired, namespace, nil, true)
		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: true,
			},
		}

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
		By("verify node is in wait-for-jobs-required' state")
		node := &corev1.Node{}
		Eventually(func() error {
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-1", Namespace: namespace}, node)
			if err != nil {
				return err
			}
			if getNodeUpgradeState(node) != upgrade.UpgradeStateWaitForJobsRequired {
				return fmt.Errorf("missing status condition")
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())

	})

	It("UpgradeStateManager move to 'upgrade-done' using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

		clusterState := withClusterUpgradeState(3, upgrade.UpgradeStateUncordonRequired, namespace,
			map[string]string{upgrade.GetUpgradeRequestedAnnotationKey(): "true"}, true)
		policy := &v1alpha1.DriverUpgradePolicySpec{
			AutoUpgrade: true,
			DrainSpec: &v1alpha1.DrainSpec{
				Enable: true,
			},
		}

		By("verify node-maintenance obj(s) have been deleted")
		nms := &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(testCtx, nms)
			return len(nms.Items) == len(clusterState.NodeStates[upgrade.UpgradeStateUncordonRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
		By("verify node is in 'upgrade-done' state")
		node := &corev1.Node{}
		Eventually(func() error {
			err := k8sClient.Get(testCtx, client.ObjectKey{Name: "node-2", Namespace: namespace}, node)
			if err != nil {
				return err
			}
			if getNodeUpgradeState(node) != upgrade.UpgradeStateDone {
				return fmt.Errorf("missing status condition")
			}
			return nil
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())

	})

	It("UpgradeStateManager should move outdated node to UpgradeRequired states with orphaned pod if 'upgrade-requested' "+
		"while using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

		orphanedPod := &corev1.Pod{}
		UnknownToUpgradeRequiredNode := NewNode("node-1").WithUpgradeState(upgrade.UpgradeStateUnknown).
			WithAnnotations(map[string]string{upgrade.GetUpgradeRequestedAnnotationKey(): "true"}).Create()
		DoneToUpgradeRequiredNode := NewNode("node-2").WithUpgradeState(upgrade.UpgradeStateDone).
			WithAnnotations(map[string]string{upgrade.GetUpgradeRequestedAnnotationKey(): "true"}).Create()

		clusterState := upgrade.NewClusterUpgradeState()
		unknownNodes := []*upgrade.NodeUpgradeState{
			{Node: UnknownToUpgradeRequiredNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		doneNodes := []*upgrade.NodeUpgradeState{
			{Node: DoneToUpgradeRequiredNode, DriverPod: orphanedPod, DriverDaemonSet: nil},
		}
		clusterState.NodeStates[upgrade.UpgradeStateUnknown] = unknownNodes
		clusterState.NodeStates[upgrade.UpgradeStateDone] = doneNodes

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(UnknownToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
		Expect(getNodeUpgradeState(DoneToUpgradeRequiredNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
	})

	It("UpgradeStateManager should move up-to-date nodes with safe driver loading annotation "+
		"to UpgradeRequired state, while using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

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

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, &v1alpha1.DriverUpgradePolicySpec{AutoUpgrade: true})).To(Succeed())
		Expect(getNodeUpgradeState(waitForSafeLoadNode)).To(Equal(upgrade.UpgradeStateUpgradeRequired))
	})
	It("UpgradeStateManager should move pod to UpgradeDone state"+
		"if it's in ValidationRequired, validation has completed, and node was initially Unschedulable "+
		"while using upgrade requestor mode", func() {
		namespace := createNamespace(fmt.Sprintf("namespace-%s", id)).Name
		cancel := withUpgradeRequestorMode(testCtx, namespace)
		defer cancel()

		node := NewNode(fmt.Sprintf("node1-%s", id)).
			WithUpgradeState(upgrade.UpgradeStateValidationRequired).
			WithAnnotations(map[string]string{upgrade.GetUpgradeInitialStateAnnotationKey(): "true"}).
			Unschedulable(true).
			Create()

		_ = NewPod(fmt.Sprintf("pod-%s", id), namespace, node.Name).
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

		commonStateManager := stateManager.WithValidationEnabled("app=validator")
		Expect(commonStateManager.IsValidationEnabled()).To(Equal(true))
		// do not mock NodeUpgradeStateProvider as it is used during ProcessUpgradeValidationRequiredNodes()
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		stateManager.NodeUpgradeStateProvider = provider

		Expect(stateManagerInterface.ApplyState(testCtx, &clusterState, policy)).To(Succeed())
		Expect(getNodeUpgradeState(node)).To(Equal(upgrade.UpgradeStateDone))
		// unschedulable annotation should be removed
		Expect(isUnschedulableAnnotationPresent(node)).To(Equal(false))
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

func removeFinalizersOrDelete(testCtx context.Context, nm *maintenancev1alpha1.NodeMaintenance) error {
	var err error
	instanceFinalizers := nm.GetFinalizers()
	if len(instanceFinalizers) == 0 {
		err = k8sClient.Delete(testCtx, nm)
		return err
	}

	nm.SetFinalizers([]string{})
	err = k8sClient.Update(testCtx, nm)
	if err != nil && k8serrors.IsNotFound(err) {
		err = nil
		Expect(err).NotTo(HaveOccurred())
	}

	return err
}

func withClusterUpgradeState(nodeCount int, nodeState string, namespace string,
	annotations map[string]string, withNodeMaintenance bool) upgrade.ClusterUpgradeState {
	clusterState := upgrade.NewClusterUpgradeState()

	daemonSet := &appsv1.DaemonSet{ObjectMeta: v1.ObjectMeta{}}
	for i := 1; i <= nodeCount; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		nodeUpgradeState := &upgrade.NodeUpgradeState{
			Node: NewNode(nodeName).WithUpgradeState(nodeState).
				WithAnnotations(annotations).Create(),
			DriverPod:       NewPod(fmt.Sprintf("pod-%d", i), namespace, nodeName).Create(),
			DriverDaemonSet: daemonSet,
		}
		if withNodeMaintenance {
			nodeUpgradeState.NodeMaintenance = NewNodeMaintenance(nodeName, namespace).Create()
		}
		clusterState.NodeStates[nodeState] =
			append(clusterState.NodeStates[nodeState], nodeUpgradeState)
	}

	return clusterState

}

func withUpgradeRequestorMode(testCtx context.Context, namespace string) context.CancelFunc {
	var err error
	os.Setenv("MAINTENANCE_OPERATOR_ENABLED", "true")
	os.Setenv("MAINTENANCE_OPERATOR_REQUESTOR_NAMESPACE", namespace)
	os.Setenv("MAINTENANCE_OPERATOR_REQUESTOR_ID", "network.opeator.com")
	_, cancelFn := context.WithCancel(testCtx)
	opts := upgrade.GetRequestorOptsFromEnvs()

	stateManagerInterface, err = upgrade.NewClusterUpgradeStateManager(log, k8sConfig,
		eventRecorder, upgrade.StateOptions{Requestor: opts})
	Expect(err).NotTo(HaveOccurred())

	stateManager, _ = stateManagerInterface.(*upgrade.ClusterUpgradeStateManagerImpl)
	provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
	stateManager.NodeUpgradeStateProvider = provider
	stateManager.DrainManager = &drainManager
	stateManager.CordonManager = &cordonManager
	stateManager.PodManager = &podManager
	stateManager.ValidationManager = &validationManager

	return cancelFn
}
