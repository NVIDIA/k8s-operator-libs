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

package requestor_test

import (
	"context"
	"fmt"
	"time"

	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/requestor"
)

var _ = Describe("NodeMaintenance Controller", func() {
	var (
		ctx                 context.Context
		currentClusterState *base.ClusterUpgradeState
		reqMngr             *requestor.UpgradeManagerImpl
		cancel              context.CancelFunc
		id                  string
	)

	policy := &v1alpha1.DriverUpgradePolicySpec{
		AutoUpgrade:         true,
		MaxParallelUpgrades: 0,
		DrainSpec: &v1alpha1.DrainSpec{
			Enable: true,
		},
		WaitForCompletion: &v1alpha1.WaitForCompletionSpec{
			PodSelector:   "",
			TimeoutSecond: 0,
		},
	}

	BeforeEach(func() {
		ctx = context.TODO()
		id = randSeq(5)
		// create node objects for scheduler to use
		By("create test nodes")
		upgradeRequired := []*base.NodeUpgradeState{
			{Node: NewNode("node1").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node2").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node3").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
			{Node: NewNode("node4").WithUpgradeState(base.UpgradeStateUpgradeRequired).Node},
		}
		upgradeUnknown := []*base.NodeUpgradeState{
			{Node: NewNode("node5").WithUpgradeState(base.UpgradeStateUnknown).Node},
		}

		currentClusterState = &base.ClusterUpgradeState{
			NodeStates: map[string][]*base.NodeUpgradeState{
				base.UpgradeStateUpgradeRequired: upgradeRequired,
				base.UpgradeStateUnknown:         upgradeUnknown,
			},
		}

		_ = NewNode("node1").WithUpgradeState(base.UpgradeStateUpgradeRequired).Create()
		_ = NewNode("node2").WithUpgradeState(base.UpgradeStateUpgradeRequired).Create()
		_ = NewNode("node3").WithUpgradeState(base.UpgradeStateUpgradeRequired).Create()
		_ = NewNode("node4").WithUpgradeState(base.UpgradeStateUpgradeRequired).Create()
		_ = NewNode("node5").WithUpgradeState(base.UpgradeStateUpgradeRequired).Create()

		// setup reconciler with manager
		By("create and start requstor upgrade manager")
		requestor.UseMaintenanceOperator = true
		requestor.MaintenanceOPControllerName = fmt.Sprintf(requestor.MaintenanceOPControllerName+"-%s", id)
		ctx, cancel = context.WithCancel(ctx)
		common, err := base.NewCommonUpgradeStateManager(log, k8sConfig, requestor.Scheme, eventRecorder)
		Expect(err).ToNot(HaveOccurred())
		m, err := requestor.NewRequestorUpgradeManagerImpl(ctx, k8sConfig, common)
		Expect(err).ToNot(HaveOccurred())
		reqMngr = m.(*requestor.UpgradeManagerImpl)
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		By("Cleanup NodeMaintenance resources")
		for _, nodeState := range currentClusterState.NodeStates[base.UpgradeStateUpgradeRequired] {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(nodeState.Node), nodeState.Node)
			if err == nil {
				// delete obj
				err = k8sClient.Delete(ctx, nodeState.Node)
				if err != nil && k8serrors.IsNotFound(err) {
					err = nil
				}
				Expect(err).ToNot(HaveOccurred())
			}
			if nodeState.NodeMaintenance != nil {
				nm := &maintenancev1alpha1.NodeMaintenance{}
				runtime.DefaultUnstructuredConverter.FromUnstructured(nodeState.NodeMaintenance.Object, nm)
				err = k8sClient.Delete(ctx, nm)
				if err != nil && k8serrors.IsNotFound(err) {
					err = nil
					Expect(err).NotTo(HaveOccurred())
				}
			}
		}
		cancel()
	})

	It("verify created node maintenance(s)", func() {
		err := reqMngr.ProcessUpgradeRequiredNodes(ctx, currentClusterState, policy)
		Expect(err).NotTo(HaveOccurred())

		nms := &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(ctx, nms)
			return len(nms.Items) == len(currentClusterState.NodeStates[base.UpgradeStateUpgradeRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())
	})

	It("node maintenance status condition check", func() {
		err := reqMngr.ProcessUpgradeRequiredNodes(ctx, currentClusterState, policy)
		Expect(err).NotTo(HaveOccurred())

		nms := &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(ctx, nms)
			return len(nms.Items) == len(currentClusterState.NodeStates[base.UpgradeStateUpgradeRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		status := maintenancev1alpha1.NodeMaintenanceStatus{
			Conditions: []metav1.Condition{{
				Type:               maintenancev1alpha1.ConditionTypeReady,
				Status:             metav1.ConditionTrue,
				Reason:             maintenancev1alpha1.ConditionReasonReady,
				Message:            "Maintenance completed successfully",
				LastTransitionTime: metav1.NewTime(time.Now()),
			}},
		}

		for _, item := range nms.Items {
			item.Status = status
			err = reqMngr.K8sClient.Status().Update(ctx, &item)

			Eventually(func() error {
				nm := &maintenancev1alpha1.NodeMaintenance{}
				err := k8sClient.Get(ctx, client.ObjectKey{Name: item.Name, Namespace: "default"}, nm)
				if err != nil {
					return err
				}
				if len(nm.Status.Conditions) == 0 {
					return fmt.Errorf("missing status condition")
				}
				return nil

			}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Eventually(func() error {
				err := k8sClient.Get(ctx, client.ObjectKey{Name: item.Name, Namespace: "default"}, node)
				if err != nil {
					return err
				}
				if getNodeUpgradeState(node) != base.UpgradeStatePostMaintenanceRequired {
					return fmt.Errorf("missing status condition")
				}
				return nil
			}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
			Expect(err).NotTo(HaveOccurred())

		}

	})
})
