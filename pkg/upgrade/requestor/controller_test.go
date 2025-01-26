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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/requestor"
)

var maxCreatedNodes = 5

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

		// setup reconciler with manager
		By("create and start requstor upgrade manager")
		requestor.MaintenanceOPControllerName = fmt.Sprintf(requestor.MaintenanceOPControllerName+"-%s", id)
		ctx, cancel = context.WithCancel(ctx)
		common, err := base.NewCommonUpgradeStateManager(log, k8sConfig, requestor.Scheme, eventRecorder)
		Expect(err).ToNot(HaveOccurred())
		opts := requestor.UpgradeRequestorQptions{
			UseMaintenanceOperator:       true,
			MaintenanceOPRequestorCordon: true,
			MaintenanceOPRequestorNS:     "default",
			MaintenanceOPRequestorID:     "nvidia.operator.com",
		}
		m, err := requestor.NewRequestorUpgradeManagerImpl(ctx, k8sConfig, common, opts)
		Expect(err).ToNot(HaveOccurred())
		reqMngr = m.(*requestor.UpgradeManagerImpl)
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		By("Cleanup NodeMaintenance resources")
		Eventually(func() bool {
			nodes := &corev1.NodeList{}
			err := k8sClient.List(ctx, nodes)
			if err != nil && k8serrors.IsNotFound(err) {
				return true
			}
			Expect(err).NotTo(HaveOccurred())
			if len(nodes.Items) == 0 {
				return true
			}
			Expect(err).NotTo(HaveOccurred())
			for _, item := range nodes.Items {
				err = k8sClient.Delete(ctx, &item)
				if err != nil && k8serrors.IsNotFound(err) {
					err = nil
				}
				Expect(err).NotTo(HaveOccurred())
			}
			return false
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		Eventually(func() bool {
			nms := &maintenancev1alpha1.NodeMaintenanceList{}
			err := k8sClient.List(ctx, nms)
			if err != nil && k8serrors.IsNotFound(err) {
				return true
			}
			Expect(err).NotTo(HaveOccurred())
			if len(nms.Items) == 0 {
				return true
			}
			for _, item := range nms.Items {
				err = removeFinalizersOrDelete(ctx, &item)
				Expect(err).NotTo(HaveOccurred())
			}
			return false
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		DeferCleanup(func() {
			By("Shut down controller manager")
			cancel()
		})
	})

	It("verify created node maintenance(s)", func() {
		By("create test nodes")
		currentClusterState = newClusterState(false, base.UpgradeStateUpgradeRequired)

		err := reqMngr.ProcessUpgradeRequiredNodes(ctx, currentClusterState, policy)
		Expect(err).NotTo(HaveOccurred())
		nms := &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(ctx, nms)
			return len(nms.Items) == len(currentClusterState.NodeStates[base.UpgradeStateUpgradeRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())
	})

	It("node maintenance status condition check", func() {
		By("create test nodes")
		currentClusterState = newClusterState(false, base.UpgradeStateUpgradeRequired)
		nodes := &corev1.NodeList{}
		Eventually(func() bool {
			k8sClient.List(ctx, nodes)
			return len(nodes.Items) == maxCreatedNodes
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		err := reqMngr.ProcessUpgradeRequiredNodes(ctx, currentClusterState, policy)
		Expect(err).NotTo(HaveOccurred())

		nms := &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(ctx, nms)
			return len(nms.Items) == len(currentClusterState.NodeStates[base.UpgradeStateUpgradeRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		for _, item := range nms.Items {
			nm := &maintenancev1alpha1.NodeMaintenance{}
			Eventually(func() bool {
				k8sClient.Get(ctx, client.ObjectKey{Name: item.Name, Namespace: "default"}, nm)
				return len(nm.Finalizers) == 0
			}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())
		}

		status := maintenancev1alpha1.NodeMaintenanceStatus{
			Conditions: []metav1.Condition{{
				Type:               maintenancev1alpha1.ConditionTypeReady,
				Status:             metav1.ConditionTrue,
				Reason:             maintenancev1alpha1.ConditionReasonReady,
				Message:            "Maintenance completed successfully",
				LastTransitionTime: metav1.NewTime(time.Now()),
			}},
		}
		By("set node-maintenance(s) finalizer")
		for _, item := range nms.Items {
			nm := &maintenancev1alpha1.NodeMaintenance{}
			err := k8sClient.Get(ctx, client.ObjectKey{Name: item.Name, Namespace: "default"}, nm)
			Expect(err).NotTo(HaveOccurred())

			nm.Finalizers = append(nm.Finalizers, requestor.MaintenanceOPFinalizerName)
			err = reqMngr.K8sClient.Update(ctx, nm)
			Expect(err).NotTo(HaveOccurred())
			condition := func(nm *maintenancev1alpha1.NodeMaintenance) bool {
				return len(nm.Finalizers) == 0
			}
			Eventually(verifyNodeMaintenanceStatus(ctx, item.Name, condition)).
				WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
		}

		for _, item := range nms.Items {
			nm := &maintenancev1alpha1.NodeMaintenance{}
			err := k8sClient.Get(ctx, client.ObjectKey{Name: item.Name, Namespace: "default"}, nm)
			Expect(err).NotTo(HaveOccurred())
			nm.Status = status
			err = reqMngr.K8sClient.Status().Update(ctx, nm)
			Expect(err).NotTo(HaveOccurred())
			conditionFn := func(nm *maintenancev1alpha1.NodeMaintenance) bool {
				return len(nm.Status.Conditions) == 0
			}
			Eventually(verifyNodeMaintenanceStatus(ctx, item.Name, conditionFn)).
				WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())

			node := &corev1.Node{}
			Eventually(func() error {
				err := k8sClient.Get(ctx, client.ObjectKey{Name: item.Spec.NodeName, Namespace: "default"}, node)
				if err != nil {
					return err
				}
				if getNodeUpgradeState(node) != base.UpgradeStatePostMaintenanceRequired {
					return fmt.Errorf("missing status condition")
				}
				return nil
			}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
			Expect(err).NotTo(HaveOccurred())
			//Eventually(verifyNodeState(ctx, item.Spec.NodeName, base.UpgradeStatePostMaintenanceRequired)).
			//	WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
		}
	})

	It("node maintenance watch deleted objects", func() {
		var err error
		By("create test nodes")
		currentClusterState = newClusterState(true, base.UpgradeStateUncordonRequired)
		nms := &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(ctx, nms)
			return len(nms.Items) == len(currentClusterState.NodeStates[base.UpgradeStateUncordonRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		err = reqMngr.ProcessUncordonRequiredNodes(ctx, currentClusterState)
		Expect(err).NotTo(HaveOccurred())

		nms = &maintenancev1alpha1.NodeMaintenanceList{}
		Eventually(func() bool {
			k8sClient.List(ctx, nms)
			return len(nms.Items) == len(currentClusterState.NodeStates[base.UpgradeStateUncordonRequired])
		}).WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(BeTrue())

		By("remove node-maintenance finalizers")
		for _, item := range nms.Items {
			nm := &maintenancev1alpha1.NodeMaintenance{}
			Expect(len(item.Finalizers)).To(Equal(1))
			err := k8sClient.Get(ctx, client.ObjectKey{Name: item.Name, Namespace: "default"}, nm)
			Expect(err).NotTo(HaveOccurred())
			nm.Finalizers = []string{}
			err = k8sClient.Update(ctx, nm)
			Expect(err).NotTo(HaveOccurred())
		}

		for _, item := range nms.Items {
			Eventually(verifyNodeState(ctx, item.Spec.NodeName, base.UpgradeStateDone)).
				WithTimeout(10 * time.Second).WithPolling(1 * 500 * time.Millisecond).Should(Succeed())
		}
	})
})

func newClusterState(shouldCreate bool, state string) *base.ClusterUpgradeState {
	var upgradeState []*base.NodeUpgradeState

	offset := randRange(100, 999)
	for i := 1; i < maxCreatedNodes; i++ {

		upgradeState = append(upgradeState, &base.NodeUpgradeState{
			Node:            NewNode(fmt.Sprintf("node%d", offset+i)).WithUpgradeState(state).Create(),
			NodeMaintenance: newNodeMaintenance(shouldCreate, offset+i),
		})
	}
	upgradeUnknown := []*base.NodeUpgradeState{
		{Node: NewNode(fmt.Sprintf("node%d", 10)).WithUpgradeState(base.UpgradeStateUnknown).Create()},
	}

	return &base.ClusterUpgradeState{
		NodeStates: map[string][]*base.NodeUpgradeState{
			state:                    upgradeState,
			base.UpgradeStateUnknown: upgradeUnknown,
		},
	}
}

func newNodeMaintenance(shouldCreate bool, id int) *unstructured.Unstructured {
	if shouldCreate {
		return NewNodeMaintenance(fmt.Sprintf("node-maintenance-%d", id), fmt.Sprintf("node%d", id)).Create()
	}
	return nil
}

func verifyNodeState(ctx context.Context, name, state string) error {
	node := &corev1.Node{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: "default"}, node)
	if err != nil {
		return err
	}
	if getNodeUpgradeState(node) != state {
		return fmt.Errorf("missing status condition")
	}
	return nil
}

func verifyNodeMaintenanceStatus(ctx context.Context, name string,
	condition func(nm *maintenancev1alpha1.NodeMaintenance) bool) error {
	nm := &maintenancev1alpha1.NodeMaintenance{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: "default"}, nm)
	if err != nil {
		return err
	}
	if condition(nm) {
		return fmt.Errorf("missing status condition")
	}
	return nil

}
