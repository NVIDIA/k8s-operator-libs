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

package common_test

import (
	"context"
	"fmt"

	common "github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/common"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("NodeUpgradeStateProvider tests", func() {
	var ctx context.Context
	var id string
	var node *corev1.Node

	BeforeEach(func() {
		ctx = context.TODO()
		id = randSeq(5)
		node = createNode(fmt.Sprintf("node-%s", id))
	})
	It("NodeUpgradeStateProvider should change node upgrade state and retrieve the latest node object", func() {
		provider := common.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)

		err := provider.ChangeNodeUpgradeState(ctx, node, common.UpgradeStateUpgradeRequired)
		Expect(err).To(Succeed())

		node, err = provider.GetNode(ctx, node.Name)
		Expect(err).To(Succeed())
		Expect(node.Labels[common.GetUpgradeStateLabelKey()]).To(Equal(common.UpgradeStateUpgradeRequired))
	})
	It("NodeUpgradeStateProvider should change node upgrade annotation and retrieve the latest node object", func() {
		provider := common.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)

		key := common.GetUpgradeInitialStateAnnotationKey()
		err := provider.ChangeNodeUpgradeAnnotation(ctx, node, key, "true")
		Expect(err).To(Succeed())

		node, err = provider.GetNode(ctx, node.Name)
		Expect(err).To(Succeed())
		Expect(node.Annotations[key]).To(Equal("true"))
	})
	It("NodeUpgradeStateProvider should delete node upgrade annotation and retrieve the latest node object", func() {
		provider := common.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)

		key := common.GetUpgradeInitialStateAnnotationKey()
		err := provider.ChangeNodeUpgradeAnnotation(ctx, node, key, "null")
		Expect(err).To(Succeed())

		node, err = provider.GetNode(ctx, node.Name)
		Expect(err).To(Succeed())
		_, exist := node.Annotations[key]
		Expect(exist).To(Equal(false))
	})
})
