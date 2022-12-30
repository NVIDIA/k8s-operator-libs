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

	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("ValidationManager", func() {
	var ctx context.Context
	var id string
	var node *corev1.Node
	var namespace *corev1.Namespace

	BeforeEach(func() {
		ctx = context.TODO()
		id = randSeq(5)
		node = createNode(fmt.Sprintf("node-%s", id))
		namespace = createNamespace(fmt.Sprintf("namespace-%s", id))
	})

	It("should return no error if podSelector is empty", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		validationManager := upgrade.NewValidationManager(k8sInterface, log, eventRecorder, provider, "")
		validationDone, err := validationManager.Validate(ctx, node)
		Expect(err).To(Succeed())
		Expect(validationDone).To(Equal(true))
	})

	It("Validate() should return false when no validation pods are running", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		validationManager := upgrade.NewValidationManager(k8sInterface, log, eventRecorder, provider, "app=validation")
		validationDone, err := validationManager.Validate(ctx, node)
		Expect(err).To(Succeed())
		Expect(validationDone).To(Equal(false))
	})

	It("Validate() should return true if validation pod is Running and Ready", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		_ = NewPod("pod", namespace.Name, node.Name).
			WithLabels(map[string]string{"app": "validator"}).
			Create()
		validationManager := upgrade.NewValidationManager(k8sInterface, log, eventRecorder, provider, "app=validator")
		validationDone, err := validationManager.Validate(ctx, node)
		Expect(err).To(Succeed())
		Expect(validationDone).To(Equal(true))
	})

	It("Validate() should return false if validation pod is Running but not Ready", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		pod := NewPod("pod", namespace.Name, node.Name).
			WithLabels(map[string]string{"app": "validator"}).
			Create()
		pod.Status.ContainerStatuses[0].Ready = false
		_ = updatePodStatus(pod)

		validationManager := upgrade.NewValidationManager(k8sInterface, log, eventRecorder, provider, "app=validator")
		validationDone, err := validationManager.Validate(ctx, node)
		Expect(err).To(Succeed())
		Expect(validationDone).To(Equal(false))
	})

	It("Validate() should return false if validation pod is not Running", func() {
		provider := upgrade.NewNodeUpgradeStateProvider(k8sClient, log, eventRecorder)
		pod := NewPod("pod", namespace.Name, node.Name).
			WithLabels(map[string]string{"app": "validator"}).
			Create()
		pod.Status.Phase = "Terminating"
		_ = updatePodStatus(pod)

		validationManager := upgrade.NewValidationManager(k8sInterface, log, eventRecorder, provider, "app=validator")
		validationDone, err := validationManager.Validate(ctx, node)
		Expect(err).To(Succeed())
		Expect(validationDone).To(Equal(false))
	})

})
