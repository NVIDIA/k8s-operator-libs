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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	upgrade "github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
)

// newStaleNodeClient returns a client whose first staleReads Node Gets return the node
// without the given label or annotation key, as a cache that has not seen the patch does.
func newStaleNodeClient(staleReads *int, removeKey func(node *corev1.Node)) client.Client {
	c, err := client.NewWithWatch(k8sConfig, client.Options{Scheme: upgrade.Scheme})
	Expect(err).NotTo(HaveOccurred())
	return interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object,
			opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if node, ok := obj.(*corev1.Node); ok && *staleReads > 0 {
				*staleReads--
				removeKey(node)
			}
			return nil
		},
	})
}

var _ = Describe("NodeUpgradeStateProvider tests", func() {
	var id string
	var node *corev1.Node

	BeforeEach(func() {
		id = randSeq(5)
		node = createNode(fmt.Sprintf("node-%s", id))
	})
	It("NodeUpgradeStateProvider should change node upgrade state and retrieve the latest node object", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)

		err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStateUpgradeRequired)
		Expect(err).To(Succeed())

		node, err = provider.GetNode(testCtx, node.Name)
		Expect(err).To(Succeed())
		Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateUpgradeRequired))
	})
	It("NodeUpgradeStateProvider should change node upgrade annotation and retrieve the latest node object", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)

		key := upgrade.GetUpgradeInitialStateAnnotationKey()
		err := provider.ChangeNodeUpgradeAnnotation(testCtx, node, key, "true")
		Expect(err).To(Succeed())

		node, err = provider.GetNode(testCtx, node.Name)
		Expect(err).To(Succeed())
		Expect(node.Annotations[key]).To(Equal("true"))
	})
	It("NodeUpgradeStateProvider should delete node upgrade annotation and retrieve the latest node object", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)

		key := upgrade.GetUpgradeInitialStateAnnotationKey()
		err := provider.ChangeNodeUpgradeAnnotation(testCtx, node, key, "null")
		Expect(err).To(Succeed())

		node, err = provider.GetNode(testCtx, node.Name)
		Expect(err).To(Succeed())
		_, exist := node.Annotations[key]
		Expect(exist).To(Equal(false))
	})
	It("NodeUpgradeStateProvider should retry quickly while the client returns the old node state", func() {
		staleReads := 2
		staleClient := newStaleNodeClient(&staleReads, func(node *corev1.Node) {
			delete(node.Labels, upgrade.GetUpgradeStateLabelKey())
		})
		provider := upgrade.NewNodeUpgradeStateProvider(staleClient, log, eventRecorder)

		start := time.Now()
		err := provider.ChangeNodeUpgradeState(testCtx, node, upgrade.UpgradeStateUpgradeRequired)
		Expect(err).To(Succeed())
		Expect(staleReads).To(Equal(0))
		// A 1 s poll interval makes two stale reads take at least 2 s.
		Expect(time.Since(start)).To(BeNumerically("<", time.Second))
		Expect(node.Labels[upgrade.GetUpgradeStateLabelKey()]).To(Equal(upgrade.UpgradeStateUpgradeRequired))
	})
	It("NodeUpgradeStateProvider should retry quickly while the client returns the old node annotation", func() {
		key := upgrade.GetUpgradeInitialStateAnnotationKey()
		staleReads := 2
		staleClient := newStaleNodeClient(&staleReads, func(node *corev1.Node) {
			delete(node.Annotations, key)
		})
		provider := upgrade.NewNodeUpgradeStateProvider(staleClient, log, eventRecorder)

		start := time.Now()
		err := provider.ChangeNodeUpgradeAnnotation(testCtx, node, key, "true")
		Expect(err).To(Succeed())
		Expect(staleReads).To(Equal(0))
		Expect(time.Since(start)).To(BeNumerically("<", time.Second))
		Expect(node.Annotations[key]).To(Equal("true"))
	})
})
