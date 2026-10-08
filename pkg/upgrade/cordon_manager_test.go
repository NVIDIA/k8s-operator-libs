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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	upgrade "github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
)

var _ = Describe("CordonManager tests", func() {
	It("CordonManager should mark a node as schedulable/unschedulable", func() {
		node := createNode("test-node")

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

	It("CordonManager should preserve a driver-manager initial-state recording when releasing its own claim", func() {
		node := NewNode("dm-initial-state-node").
			WithAnnotations(map[string]string{upgrade.DriverManagerInitialUnschedulableAnnotation: "false"}).
			Unschedulable(true).
			Create()

		cordonManager := upgrade.NewCordonManager(k8sInterface, log)
		Expect(cordonManager.Cordon(testCtx, node)).To(Succeed())
		Expect(node.Annotations).To(HaveKeyWithValue(upgrade.UpgradeControllerCordonClaimAnnotation, "true"))
		Expect(cordonManager.Uncordon(testCtx, node)).To(Succeed())

		Expect(node.Spec.Unschedulable).To(BeTrue())
		Expect(node.Annotations).To(HaveKeyWithValue(upgrade.DriverManagerInitialUnschedulableAnnotation, "false"))
		Expect(node.Annotations).NotTo(HaveKey(upgrade.UpgradeControllerCordonClaimAnnotation))
	})
})
