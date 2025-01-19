/*
 Copyright 2024, NVIDIA CORPORATION & AFFILIATES

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
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/exp/rand"

	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/requestor"

	randv2 "math/rand/v2"

	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	//+kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var k8sConfig *rest.Config
var k8sClient client.Client
var k8sInterface kubernetes.Interface
var testEnv *envtest.Environment
var log logr.Logger
var eventRecorder = record.NewFakeRecorder(100)
var createdObjects []client.Object

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
	}

	var err error
	k8sConfig, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sConfig).NotTo(BeNil())

	err = maintenancev1alpha1.AddToScheme(requestor.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// +kubebuilder:scaffold:scheme

	k8sClient, err = client.New(k8sConfig, client.Options{Scheme: requestor.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	k8sInterface, err = kubernetes.NewForConfig(k8sConfig)
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sInterface).NotTo(BeNil())

	// set driver name to be managed by the upgrade-manager
	base.SetDriverName("gpu")

	log = ctrl.Log.WithName("requestorSuitTest")
	// init operator vars
	requestor.MaintenanceOPRequestorNS = "default"

})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	log.Error(err, "failed test")
	Expect(err).NotTo(HaveOccurred())
})

var _ = BeforeEach(func() {
	createdObjects = nil
})

var _ = AfterEach(func() {
	for i := range createdObjects {
		r := createdObjects[i]
		key := client.ObjectKeyFromObject(r)
		err := k8sClient.Get(context.TODO(), key, r)
		if err == nil {
			Expect(k8sClient.Delete(context.TODO(), r)).To(Succeed())
		}
		// drain events from FakeRecorder
		for len(eventRecorder.Events) > 0 {
			<-eventRecorder.Events
		}
		_, isNamespace := r.(*corev1.Namespace)
		if !isNamespace {
			Eventually(func() error {
				return k8sClient.Get(context.TODO(), key, r)
			}).Should(HaveOccurred())
		}
	}
})

type Node struct {
	*corev1.Node
}

func NewNode(name string) Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{"dummy-key": "dummy-value"},
			Annotations: map[string]string{"dummy-key": "dummy-value"},
		},
	}
	Expect(node.Labels).NotTo(BeNil())
	return Node{node}
}

func (n Node) WithUpgradeState(state string) Node {
	if n.Labels == nil {
		n.Labels = make(map[string]string)
	}
	n.Labels[base.GetUpgradeStateLabelKey()] = state
	return n
}

func (n Node) WithLabels(l map[string]string) Node {
	n.Labels = l
	return n
}

func (n Node) WithAnnotations(a map[string]string) Node {
	n.Annotations = a
	return n
}

func (n Node) Unschedulable(b bool) Node {
	n.Spec.Unschedulable = b
	return n
}

func (n Node) Create() *corev1.Node {
	node := n.Node
	err := k8sClient.Create(context.TODO(), node)
	Expect(err).NotTo(HaveOccurred())
	createdObjects = append(createdObjects, node)
	return node
}

type NodeMaintenance struct {
	*maintenancev1alpha1.NodeMaintenance
}

func NewNodeMaintenance(name, nodeName string) NodeMaintenance {
	status := maintenancev1alpha1.NodeMaintenanceStatus{
		Conditions: []metav1.Condition{{
			Type:               maintenancev1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionTrue,
			Reason:             maintenancev1alpha1.ConditionReasonReady,
			Message:            "Maintenance completed successfully",
			LastTransitionTime: metav1.NewTime(time.Now()),
		}},
	}
	nm := &maintenancev1alpha1.NodeMaintenance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Labels:     map[string]string{"hello": "world"},
			Finalizers: []string{requestor.MaintenanceOPFinalizerName},
		},
		Spec: maintenancev1alpha1.NodeMaintenanceSpec{
			RequestorID: requestor.MaintenanceOPRequestorID,
			NodeName:    nodeName,
		},
		Status: status,
	}
	Expect(nm.Status.Conditions).NotTo(BeNil())
	return NodeMaintenance{nm}
}

func (n NodeMaintenance) Create() *unstructured.Unstructured {
	nm := n.NodeMaintenance
	err := k8sClient.Create(context.TODO(), nm)
	Expect(err).NotTo(HaveOccurred())
	createdObjects = append(createdObjects, nm)

	obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(nm)
	return &unstructured.Unstructured{Object: obj}
}

func removeFinalizersOrDelete(ctx context.Context, nm *maintenancev1alpha1.NodeMaintenance) error {
	var err error
	instanceFinalizers := nm.GetFinalizers()
	if len(instanceFinalizers) == 0 {
		err = k8sClient.Delete(ctx, nm)
		return err
	}

	nm.SetFinalizers([]string{})
	err = k8sClient.Update(ctx, nm)
	if err != nil && k8serrors.IsNotFound(err) {
		err = nil
		Expect(err).NotTo(HaveOccurred())
	}

	return err
}

func getNodeUpgradeState(node *corev1.Node) string {
	return node.Labels[base.GetUpgradeStateLabelKey()]
}

func randSeq(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randRange(min, max int) int {
	return randv2.IntN(max-min) + min
}
