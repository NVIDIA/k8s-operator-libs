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
	"slices"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type recordingPodManager struct {
	PodManager
	scheduled []*corev1.Pod
}

func (m *recordingPodManager) SchedulePodsRestart(_ context.Context, pods []*corev1.Pod) error {
	m.scheduled = pods
	return nil
}

func newPodRestartManager(t *testing.T, maxParallelUpgrades int) (*CommonUpgradeManagerImpl, *recordingPodManager) {
	t.Helper()
	podManager := &recordingPodManager{}
	return &CommonUpgradeManagerImpl{
		Log:                 logr.Discard(),
		PodManager:          podManager,
		maxParallelUpgrades: maxParallelUpgrades,
	}, podManager
}

// newPodRestartState builds PodRestartRequired nodes with orphaned pods, which skip
// the DaemonSet revision hash comparison and go straight to the deletion decision.
func newPodRestartState(nodes int, terminating map[int]bool) *ClusterUpgradeState {
	state := NewClusterUpgradeState()
	for i := 0; i < nodes; i++ {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("driver-pod-%d", i),
			Namespace: "gpu-operator",
		}}
		if terminating[i] {
			now := metav1.Now()
			pod.DeletionTimestamp = &now
		}
		state.NodeStates[UpgradeStatePodRestartRequired] = append(
			state.NodeStates[UpgradeStatePodRestartRequired],
			&NodeUpgradeState{
				Node:      &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i)}},
				DriverPod: pod,
			})
	}
	return &state
}

func scheduledNames(pods []*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return names
}

func TestProcessPodRestartNodesLimitsDeletionsPerPass(t *testing.T) {
	manager, podManager := newPodRestartManager(t, 2)

	if err := manager.ProcessPodRestartNodes(context.Background(), newPodRestartState(5, nil)); err != nil {
		t.Fatalf("ProcessPodRestartNodes() error = %v", err)
	}
	if got := len(podManager.scheduled); got != 2 {
		t.Fatalf("scheduled %d pod deletions, want 2", got)
	}
}

func TestProcessPodRestartNodesCountsRestartingPods(t *testing.T) {
	manager, podManager := newPodRestartManager(t, 2)

	if err := manager.ProcessPodRestartNodes(context.Background(),
		newPodRestartState(4, map[int]bool{0: true})); err != nil {
		t.Fatalf("ProcessPodRestartNodes() error = %v", err)
	}
	if got := len(podManager.scheduled); got != 1 {
		t.Fatalf("scheduled %d pod deletions with one pod already restarting, want 1", got)
	}

	if err := manager.ProcessPodRestartNodes(context.Background(),
		newPodRestartState(4, map[int]bool{0: true, 1: true})); err != nil {
		t.Fatalf("ProcessPodRestartNodes() error = %v", err)
	}
	if got := len(podManager.scheduled); got != 0 {
		t.Fatalf("scheduled %d pod deletions with all slots busy, want 0", got)
	}
}

func TestProcessPodRestartNodesNeverReschedulesTerminatingPods(t *testing.T) {
	manager, podManager := newPodRestartManager(t, 0)

	if err := manager.ProcessPodRestartNodes(context.Background(),
		newPodRestartState(3, map[int]bool{1: true})); err != nil {
		t.Fatalf("ProcessPodRestartNodes() error = %v", err)
	}
	want := []string{"driver-pod-0", "driver-pod-2"}
	if got := scheduledNames(podManager.scheduled); !slices.Equal(got, want) {
		t.Fatalf("scheduled pods %v, want %v", got, want)
	}
}

func TestProcessPodRestartNodesUnlimited(t *testing.T) {
	manager, podManager := newPodRestartManager(t, 0)

	if err := manager.ProcessPodRestartNodes(context.Background(), newPodRestartState(5, nil)); err != nil {
		t.Fatalf("ProcessPodRestartNodes() error = %v", err)
	}
	if got := len(podManager.scheduled); got != 5 {
		t.Fatalf("scheduled %d pod deletions in unlimited mode, want 5", got)
	}
}
