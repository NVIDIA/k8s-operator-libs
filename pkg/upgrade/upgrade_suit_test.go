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
	"math/rand"
	"testing"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/envtest/printer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/mocks"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var k8sClient client.Client
var k8sInterface kubernetes.Interface
var testEnv *envtest.Environment
var log logr.Logger
var nodeUpgradeStateProvider mocks.NodeUpgradeStateProvider
var drainManager mocks.DrainManager
var podManager mocks.PodManager
var cordonManager mocks.CordonManager
var eventRecorder = record.NewFakeRecorder(100)

var createdObjects []client.Object

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecsWithDefaultAndCustomReporters(t,
		"Controller Suite",
		[]Reporter{printer.NewlineReporter{}})
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{}

	cfg, err := testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// +kubebuilder:scaffold:scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	k8sInterface, err = kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sInterface).NotTo(BeNil())

	log = ctrl.Log.WithName("upgradeSuitTest")

	// set driver name to be managed by the upgrade-manager
	upgrade.SetDriverName("gpu")

	nodeUpgradeStateProvider = mocks.NodeUpgradeStateProvider{}
	nodeUpgradeStateProvider.
		On("ChangeNodeUpgradeState", mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, node *corev1.Node, newNodeState string) error {
			node.Labels[upgrade.GetUpgradeStateLabelKey()] = newNodeState
			return nil
		})

	drainManager = mocks.DrainManager{}
	drainManager.
		On("ScheduleNodesDrain", mock.Anything, mock.Anything).
		Return(nil)
	podManager = mocks.PodManager{}
	podManager.
		On("SchedulePodsRestart", mock.Anything, mock.Anything).
		Return(nil)
	podManager.
		On("ScheduleCheckOnPodCompletion", mock.Anything, mock.Anything).
		Return(nil)
	podManager.
		On("SchedulePodEviction", mock.Anything, mock.Anything).
		Return(nil)
	cordonManager = mocks.CordonManager{}
	cordonManager.
		On("Cordon", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	cordonManager.
		On("Uncordon", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
}, 60)

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
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

type Pod struct {
	*corev1.Pod
}

func NewPod(name, namespace, nodeName string) Pod {
	gracePeriodSeconds := int64(0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &gracePeriodSeconds,
			NodeName:                      nodeName,
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "test-image",
				},
			},
		},
	}

	return Pod{pod}
}

func (p Pod) WithLabels(labels map[string]string) Pod {
	p.ObjectMeta.Labels = labels
	return p
}

func (p Pod) WithEmptyDir() Pod {
	p.Spec.Volumes = []corev1.Volume{
		corev1.Volume{
			Name: "volume",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	return p
}

func (p Pod) WithResource(name, quantity string) Pod {
	resourceQuantity, err := resource.ParseQuantity(quantity)
	Expect(err).NotTo(HaveOccurred())
	p.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName(name): resourceQuantity,
		},
	}
	return p
}

func (p Pod) Create() *corev1.Pod {
	pod := p.Pod
	err := k8sClient.Create(context.TODO(), pod)
	Expect(err).NotTo(HaveOccurred())

	// set Pod in Running state
	pod.Status.Phase = corev1.PodRunning
	err = k8sClient.Status().Update(context.TODO(), pod)
	Expect(err).NotTo(HaveOccurred())

	createdObjects = append(createdObjects, pod)
	return pod
}

func createNamespace(name string) *corev1.Namespace {
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := k8sClient.Create(context.TODO(), namespace)
	Expect(err).NotTo(HaveOccurred())
	createdObjects = append(createdObjects, namespace)
	return namespace
}

func createPod(name, namespace string, labels map[string]string, nodeName string) *corev1.Pod {
	gracePeriodSeconds := int64(0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: &gracePeriodSeconds,
			NodeName:                      nodeName,
			Containers: []corev1.Container{
				{
					Name:  "test-container",
					Image: "test-image",
				},
			},
		},
	}
	err := k8sClient.Create(context.TODO(), pod)
	Expect(err).NotTo(HaveOccurred())
	createdObjects = append(createdObjects, pod)
	return pod
}

func updatePodStatus(pod *corev1.Pod) error {
	err := k8sClient.Status().Update(context.TODO(), pod)
	Expect(err).NotTo(HaveOccurred())
	return err
}

func updatePod(pod *corev1.Pod) error {
	err := k8sClient.Update(context.TODO(), pod)
	Expect(err).NotTo(HaveOccurred())
	return err
}

func createJob(name string, namespace string, labels map[string]string) *batchv1.Job {
	var backOffLimit int32 = 0
	manualSelector := true
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			ManualSelector: &manualSelector,
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    name,
							Image:   "test-image",
							Command: []string{"test-command"},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
			BackoffLimit: &backOffLimit,
		},
	}
	err := k8sClient.Create(context.TODO(), job)
	Expect(err).NotTo(HaveOccurred())
	createdObjects = append(createdObjects, job)
	return job
}

func createNode(name string) *corev1.Node {
	node := &corev1.Node{}
	node.Name = name
	err := k8sClient.Create(context.TODO(), node)
	Expect(err).NotTo(HaveOccurred())
	createdObjects = append(createdObjects, node)
	return node
}

func getNode(name string) *corev1.Node {
	node := &corev1.Node{}
	err := k8sClient.Get(context.TODO(), types.NamespacedName{Name: name}, node)
	Expect(err).NotTo(HaveOccurred())
	Expect(node).NotTo(BeNil())
	return node
}

func updateNode(node *corev1.Node) error {
	err := k8sClient.Update(context.TODO(), node)
	Expect(err).NotTo(HaveOccurred())
	return err
}

func deleteObj(obj client.Object) {
	Expect(k8sClient.Delete(context.TODO(), obj)).To(BeNil())
}

func getNodeUpgradeState(node *corev1.Node) string {
	return node.Labels[upgrade.GetUpgradeStateLabelKey()]
}

func randSeq(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
