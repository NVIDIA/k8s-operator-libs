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

package upgrade

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubectl/pkg/drain"

	v1alpha1 "github.com/NVIDIA/k8s-operator-libs/api"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
)

// PodManagerImpl implements PodManager interface and checks for pod states
type PodManagerImpl struct {
	k8sInterface             kubernetes.Interface
	nodeUpgradeStateProvider NodeUpgradeStateProvider
	podDeletionFilter        PodDeletionFilter
	nodesInProgress          *StringSet
	log                      logr.Logger
	eventRecorder            record.EventRecorder
}

// PodManager is an interface that allows to wait on certain pod statuses
type PodManager interface {
	ScheduleCheckOnPodCompletion(context.Context, *PodManagerConfig) error
	SchedulePodsRestart(context.Context, []*v1.Pod) error
	SchedulePodEviction(context.Context, *PodManagerConfig) error
}

// PodManagerConfig represent the selector for pods and Node names to be considered for managing those pods
type PodManagerConfig struct {
	Selector     string
	Nodes        []*v1.Node
	DeletionSpec *v1alpha1.PodDeletionSpec
}

// PodDeletionFilter takes a pod and returns a boolean indicating whether the pod should be deleted
type PodDeletionFilter func(corev1.Pod) bool

// SchedulePodEviction receives a config for pod eviction and deletes pods for each node in the list.
// The set of pods to delete is determined by a filter that is provided to the PodManagerImpl during construction.
func (m *PodManagerImpl) SchedulePodEviction(ctx context.Context, config *PodManagerConfig) error {
	m.log.V(consts.LogLevelInfo).Info("Starting Pod Deletion")

	if len(config.Nodes) == 0 {
		m.log.V(consts.LogLevelInfo).Info("No nodes scheduled for pod deletion")
		return nil
	}

	podDeletionSpec := config.DeletionSpec

	if podDeletionSpec == nil {
		return fmt.Errorf("pod deletion spec should not be empty")
	}

	// Create a custom drain filter which will be passed to the drain helper.
	// The drain helper will carry out the actual deletion of pods on a node.
	customDrainFilter := func(pod corev1.Pod) drain.PodDeleteStatus {
		delete := m.podDeletionFilter(pod)
		if !delete {
			return drain.MakePodDeleteStatusSkip()
		}
		return drain.MakePodDeleteStatusOkay()
	}

	drainHelper := drain.Helper{
		Ctx:                 ctx,
		Client:              m.k8sInterface,
		Out:                 os.Stdout,
		ErrOut:              os.Stderr,
		GracePeriodSeconds:  -1,
		IgnoreAllDaemonSets: true,
		DeleteEmptyDirData:  podDeletionSpec.DeleteEmptyDir,
		Force:               podDeletionSpec.Force,
		Timeout:             time.Duration(podDeletionSpec.TimeoutSecond) * time.Second,
		AdditionalFilters:   []drain.PodFilter{customDrainFilter},
	}

	var wg sync.WaitGroup
	for _, node := range config.Nodes {
		if !m.nodesInProgress.Has(node.Name) {
			m.log.V(consts.LogLevelInfo).Info("Deleting pods on node", "node", node.Name)
			m.nodesInProgress.Add(node.Name)

			// Increment the WaitGroup counter.
			wg.Add(1)
			go func(node corev1.Node) {
				// Decrement the counter when the goroutine completes.
				defer wg.Done()
				defer m.nodesInProgress.Remove(node.Name)

				m.log.V(consts.LogLevelInfo).Info("Identifying pods to delete", "node", node.Name)

				// List all pods
				podList, err := m.ListPods(ctx, "", node.Name)
				if err != nil {
					m.log.V(consts.LogLevelError).Error(err, "Failed to list pods", "node", node.Name)
					return
				}

				// Get number of pods requiring deletion using the podDeletionFilter
				numPodsToDelete := 0
				for _, pod := range podList.Items {
					if m.podDeletionFilter(pod) == true {
						numPodsToDelete += 1
					}
				}

				if numPodsToDelete == 0 {
					m.log.V(consts.LogLevelInfo).Info("No pods require deletion", "node", node.Name)
					_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, &node, UpgradeStateDrainRequired)
					return
				}

				m.log.V(consts.LogLevelInfo).Info("Identifying which pods can be deleted", "node", node.Name)
				podDeleteList, errs := drainHelper.GetPodsForDeletion(node.Name)

				numPodsCanDelete := len(podDeleteList.Pods())
				if numPodsCanDelete != numPodsToDelete {
					m.log.V(consts.LogLevelError).Error(nil, "Cannot delete all required pods", "node", node.Name)
					if errs != nil {
						for _, err := range errs {
							m.log.V(consts.LogLevelError).Error(err, "Error reported by drain helper", "node", node.Name)
						}
					}
					_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, &node, UpgradeStateFailed)
					return
				}

				for _, p := range podDeleteList.Pods() {
					m.log.V(consts.LogLevelInfo).Info("Identified pod to delete", "node", node.Name, "namespace", p.Namespace, "name", p.Name)
				}
				m.log.V(consts.LogLevelDebug).Info("Warnings when identifying pods to delete", "warnings", podDeleteList.Warnings(), "node", node.Name)

				err = drainHelper.DeleteOrEvictPods(podDeleteList.Pods())
				if err != nil {
					m.log.V(consts.LogLevelError).Error(err, "Failed to delete pods on the node", "node", node.Name)
					_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, &node, UpgradeStateFailed)
					m.eventRecorder.Eventf(&node, corev1.EventTypeWarning, GetEventReason(), "Failed to delete workload pods on the node for the driver upgrade, %s", err.Error())
					return
				}

				m.log.V(consts.LogLevelInfo).Info("Deleted pods on the node", "node", node.Name)
				_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, &node, UpgradeStateDrainRequired)
				m.eventRecorder.Event(&node, corev1.EventTypeNormal, GetEventReason(), "Deleted workload pods on the node for the driver upgrade")
			}(*node)
		} else {
			m.log.V(consts.LogLevelInfo).Info("Node is already getting pods deleted, skipping", "node", node.Name)
		}
	}
	// Wait for all goroutines to complete
	wg.Wait()
	return nil
}

// SchedulePodsRestart receives a list of pods and schedules to delete them
// TODO, schedule deletion of pods in parallel on all nodes
func (m *PodManagerImpl) SchedulePodsRestart(ctx context.Context, pods []*v1.Pod) error {
	m.log.V(consts.LogLevelInfo).Info("Starting Pod Delete")
	if len(pods) == 0 {
		m.log.V(consts.LogLevelInfo).Info("No pods scheduled to restart")
		return nil
	}
	for _, pod := range pods {
		m.log.V(consts.LogLevelInfo).Info("Deleting pod", "pod", pod.Name)
		deleteOptions := meta_v1.DeleteOptions{}
		err := m.k8sInterface.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, deleteOptions)
		if err != nil {
			m.log.V(consts.LogLevelInfo).Error(err, "Failed to delete pod", "pod", pod.Name)
			m.eventRecorder.Eventf(pod, corev1.EventTypeWarning, GetEventReason(), "Failed to restart driver pod %s", err.Error())
			return err
		}
	}
	return nil
}

// ScheduleCheckOnPodCompletion receives PodSelectorConfig and schedules checks for pod statuses on each node in the list.
// If the checks are successful, the node moves to UpgradeStatePodDeletionRequired state,
// otherwise it will stay in the same current state.
func (m *PodManagerImpl) ScheduleCheckOnPodCompletion(ctx context.Context, config *PodManagerConfig) error {
	m.log.V(consts.LogLevelInfo).Info("Pod Manager, starting checks on pod statuses")
	var wg sync.WaitGroup

	for _, node := range config.Nodes {
		m.log.V(consts.LogLevelInfo).Info("Schedule checks for pod completion", "node", node.Name)
		// fetch the pods using the label selector provided
		podList, err := m.ListPods(ctx, config.Selector, node.Name)
		if err != nil {
			m.log.V(consts.LogLevelError).Error(err, "Failed to list pods", "selector", config.Selector, "node", node.Name)
			return err
		}
		if len(podList.Items) > 0 {
			m.log.V(consts.LogLevelDebug).Error(err, "Found workload pods", "selector", config.Selector, "node", node.Name, "pods", len(podList.Items))
		}
		// Increment the WaitGroup counter.
		wg.Add(1)
		go func(node corev1.Node) {
			// Decrement the counter when the goroutine completes.
			defer wg.Done()
			running := false
			for _, pod := range podList.Items {
				running = m.IsPodRunning(pod)
				if running {
					break
				}
			}
			// ignore the state update even if single workload pod is running on the node
			if running {
				m.log.V(consts.LogLevelInfo).Info("Workload pods are still running on the node", "node", node.Name)
				return
			}
			// update node state
			_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, &node, UpgradeStatePodDeletionRequired)
			m.log.V(consts.LogLevelInfo).Info("Updated the node state", "node", node.Name, "state", UpgradeStatePodDeletionRequired)
		}(*node)
	}
	// Wait for all goroutines to complete
	wg.Wait()
	return nil
}

// ListPods returns the list of pods in all namespaces with the given selector
func (m *PodManagerImpl) ListPods(ctx context.Context, selector string, nodeName string) (*v1.PodList, error) {
	listOptions := meta_v1.ListOptions{LabelSelector: selector, FieldSelector: "spec.nodeName=" + nodeName}
	podList, err := m.k8sInterface.CoreV1().Pods("").List(ctx, listOptions)
	if err != nil {
		return nil, err
	}
	return podList, nil
}

// IsPodRunning returns true when the given pod is currently running
func (m *PodManagerImpl) IsPodRunning(pod v1.Pod) bool {
	switch pod.Status.Phase {
	case v1.PodRunning:
		m.log.V(consts.LogLevelDebug).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName, "state", v1.PodRunning)
		return true
	case v1.PodFailed:
		m.log.V(consts.LogLevelInfo).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName, "state", v1.PodFailed)
		return false
	case v1.PodSucceeded:
		m.log.V(consts.LogLevelInfo).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName, "state", v1.PodSucceeded)
		return false
	case v1.PodPending:
		m.log.V(consts.LogLevelInfo).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName, "state", v1.PodPending)
		return false
	}
	return false
}

// NewPodManager returns an instance of PodManager implementation
func NewPodManager(
	k8sInterface kubernetes.Interface,
	nodeUpgradeStateProvider NodeUpgradeStateProvider,
	log logr.Logger,
	podDeletionFilter PodDeletionFilter,
	eventRecorder record.EventRecorder) *PodManagerImpl {
	mgr := &PodManagerImpl{
		k8sInterface:             k8sInterface,
		log:                      log,
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
		podDeletionFilter:        podDeletionFilter,
		nodesInProgress:          NewStringSet(),
		eventRecorder:            eventRecorder,
	}

	return mgr
}
