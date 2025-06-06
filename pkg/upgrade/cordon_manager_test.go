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

		err = cordonManager.Uncordon(testCtx, node)
		Expect(err).To(Succeed())
		Expect(node.Spec.Unschedulable).To(BeFalse())
	})
})
