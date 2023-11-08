/*
Copyright 2023 NVIDIA CORPORATION & AFFILIATES

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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"

	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
)

var _ = Describe("SafeDriverLoadManager", func() {
	var (
		node *corev1.Node
		ctx  context.Context
		id   string
		mgr  upgrade.SafeDriverLoadManager
	)
	BeforeEach(func() {
		ctx = context.Background()
		// generate random id for test
		id = randSeq(5)
		// create k8s objects
		node = createNode(fmt.Sprintf("node-%s", id))
		mgr = upgrade.NewSafeDriverLoadManager(upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder), log)
	})
	It("IsWaitingForSafeDriverLoad", func() {
		annotationKey := upgrade.GetUpgradeDriverWaitForSafeLoadAnnotationKey()
		Expect(k8sClient.Patch(
			ctx, node, client.RawPatch(types.StrategicMergePatchType,
				[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q: "true"}}}`,
					annotationKey))))).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: node.Name}, node)).NotTo(HaveOccurred())
		Expect(mgr.IsWaitingForSafeDriverLoad(ctx, node)).To(BeTrue())
		Expect(k8sClient.Patch(
			ctx, node, client.RawPatch(types.StrategicMergePatchType,
				[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q: null}}}`,
					annotationKey))))).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: node.Name}, node)).NotTo(HaveOccurred())
		Expect(mgr.IsWaitingForSafeDriverLoad(ctx, node)).To(BeFalse())
	})
	It("UnblockLoading", func() {
		annotationKey := upgrade.GetUpgradeDriverWaitForSafeLoadAnnotationKey()
		Expect(k8sClient.Patch(
			ctx, node, client.RawPatch(types.StrategicMergePatchType,
				[]byte(fmt.Sprintf(`{"metadata":{"annotations":{%q: "true"}}}`,
					annotationKey))))).NotTo(HaveOccurred())
		Expect(mgr.UnblockLoading(ctx, node)).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: node.Name}, node)).NotTo(HaveOccurred())
		Expect(node.Annotations[annotationKey]).To(BeEmpty())
		// should not fail when called on non blocked node
		Expect(mgr.UnblockLoading(ctx, node)).NotTo(HaveOccurred())
	})
})
