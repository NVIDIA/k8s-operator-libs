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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	upgrade "github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
)

func setUnschedulableAs(node *corev1.Node, fieldManager string, unschedulable bool) *corev1.Node {
	patch := fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, unschedulable)
	updated, err := k8sInterface.CoreV1().Nodes().Patch(context.TODO(), node.Name,
		types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{FieldManager: fieldManager})
	Expect(err).NotTo(HaveOccurred())
	return updated
}

func removeAnnotationAs(node *corev1.Node, fieldManager, key string) *corev1.Node {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, key)
	updated, err := k8sInterface.CoreV1().Nodes().Patch(context.TODO(), node.Name,
		types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{FieldManager: fieldManager})
	Expect(err).NotTo(HaveOccurred())
	return updated
}

var _ = Describe("CordonManager tests", func() {
	It("CordonManager should mark a node as schedulable/unschedulable", func() {
		// A node created without fields never gets managedFields, unlike kubelet-registered nodes.
		node := NewNode("test-node").Create()

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		err := cordonManager.Cordon(testCtx, node)
		Expect(err).To(Succeed())
		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations).To(HaveKeyWithValue(upgrade.UpgradeControllerCordonClaimAnnotation, "true"))
		err = cordonManager.Uncordon(testCtx, node)
		Expect(err).To(Succeed())
		Expect(node.Spec.Unschedulable).To(BeFalse())
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})

	It("CordonManager should preserve the driver-manager claim when releasing its own", func() {
		node := NewNode("shared-cordon-node").
			WithAnnotations(map[string]string{upgrade.DriverManagerCordonClaimAnnotation: "true"}).
			Create()

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(Succeed())
		Expect(cordonManager.Uncordon(testCtx, node)).To(Succeed())

		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations).To(HaveKeyWithValue(upgrade.DriverManagerCordonClaimAnnotation, "true"))
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})

	It("CordonManager should not claim a cordon recorded as external", func() {
		node := NewNode("external-cordon-node").
			WithAnnotations(map[string]string{upgrade.GetUpgradeInitialStateAnnotationKey(): "true"}).
			Unschedulable(true).
			Create()

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(Succeed())
		Expect(cordonManager.Uncordon(testCtx, node)).To(Succeed())

		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})

	It("CordonManager should refuse an administrator cordon", func() {
		node := NewNode("admin-cordon-node").Unschedulable(true).CreateAs("kubectl-cordon")

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(MatchError(upgrade.ErrNodeCordonedExternally))

		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})

	It("CordonManager should not treat a driver-manager \"true\" recording as a claim", func() {
		node := NewNode("dm-recorded-admin-cordon-node").
			WithAnnotations(map[string]string{upgrade.DriverManagerInitialUnschedulableAnnotation: "true"}).
			Unschedulable(true).
			CreateAs(upgrade.DriverManagerFieldManager)

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(MatchError(upgrade.ErrNodeCordonedExternally))
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})

	It("CordonManager should drop a stale claim on a cordon someone else re-applied", func() {
		node := NewNode("reapplied-cordon-node").Create()

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(Succeed())
		// kubectl uncordon leaves the claim annotation behind.
		setUnschedulableAs(node, "kubectl-cordon", false)
		node = setUnschedulableAs(node, "kubectl-cordon", true)
		Expect(node.Annotations).To(HaveKeyWithValue(upgrade.UpgradeControllerCordonClaimAnnotation, "true"))

		Expect(cordonManager.Cordon(testCtx, node)).To(MatchError(upgrade.ErrNodeCordonedExternally))
		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})

	It("CordonManager should leave a cordon re-applied mid-upgrade in place on uncordon", func() {
		node := NewNode("reapplied-mid-upgrade-node").Create()

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(Succeed())
		setUnschedulableAs(node, "kubectl-cordon", false)
		node = setUnschedulableAs(node, "kubectl-cordon", true)

		Expect(cordonManager.Uncordon(testCtx, node)).To(Succeed())
		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})

	It("CordonManager should uncordon when driver-manager has handed its cordon over", func() {
		node := NewNode("dm-handoff-node").
			WithAnnotations(map[string]string{
				upgrade.DriverManagerCordonClaimAnnotation:          "true",
				upgrade.DriverManagerInitialUnschedulableAnnotation: "false",
			}).
			Unschedulable(true).
			CreateAs(upgrade.DriverManagerFieldManager)

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(Succeed())
		Expect(node.Annotations).To(HaveKeyWithValue(upgrade.UpgradeControllerCordonClaimAnnotation, "true"))

		// driver-manager releases while the controller holds the cordon and keeps its "false" recording.
		node = removeAnnotationAs(node, upgrade.DriverManagerFieldManager, upgrade.DriverManagerCordonClaimAnnotation)

		Expect(cordonManager.Uncordon(testCtx, node)).To(Succeed())
		Expect(node.Spec.Unschedulable).To(BeFalse())
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
		Expect(node.Annotations).NotTo(HaveKey(upgrade.DriverManagerInitialUnschedulableAnnotation))
	})

	It("CordonManager should uncordon a node cordoned by a controller without the claim annotation", func() {
		node := NewNode("legacy-controller-cordon-node").Unschedulable(true).CreateAs("gpu-operator")

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Uncordon(testCtx, node)).To(Succeed())
		Expect(node.Spec.Unschedulable).To(BeFalse())
	})

	It("CordonManager should accept a requestor-mode claim on a maintenance operator cordon", func() {
		node := NewNode("requestor-cordon-node").
			WithAnnotations(map[string]string{
				upgrade.UpgradeControllerCordonClaimAnnotation: "true",
				upgrade.GetUpgradeRequestorModeAnnotationKey(): "true",
			}).
			Unschedulable(true).
			CreateAs("maintenance-operator")

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(Succeed())
		Expect(node.Annotations).To(HaveKeyWithValue(upgrade.UpgradeControllerCordonClaimAnnotation, "true"))
	})
})
