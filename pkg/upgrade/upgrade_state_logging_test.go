/*
Copyright 2026 NVIDIA CORPORATION & AFFILIATES

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
	"testing"

	"github.com/go-logr/logr/funcr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLogNodeStatesOnlyWhenCountsChange(t *testing.T) {
	logCount := 0
	logger := funcr.New(func(_, _ string) { logCount++ }, funcr.Options{})
	manager := &ClusterUpgradeStateManagerImpl{
		CommonUpgradeManagerImpl: &CommonUpgradeManagerImpl{Log: logger},
	}

	stateWithPod := func(state string, owner *appsv1.DaemonSet) ClusterUpgradeState {
		return ClusterUpgradeState{
			NodeStates: map[string][]*NodeUpgradeState{
				state: {{DriverPod: &corev1.Pod{}, DriverDaemonSet: owner}},
			},
		}
	}
	owner := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{UID: "driver"}}

	passes := []struct {
		name     string
		state    ClusterUpgradeState
		wantLogs int
	}{
		{"first pass with zero counts", NewClusterUpgradeState(), 1},
		{"unchanged zero counts", NewClusterUpgradeState(), 1},
		{"orphaned pod on a node waiting for drain", stateWithPod(UpgradeStateDrainRequired, nil), 2},
		{"same total, different node state", stateWithPod(UpgradeStatePodRestartRequired, nil), 3},
		{"same node state, different pod ownership", stateWithPod(UpgradeStatePodRestartRequired, owner), 4},
		{"unchanged owned pod counts", stateWithPod(UpgradeStatePodRestartRequired, owner.DeepCopy()), 4},
		{"return to initial counts", NewClusterUpgradeState(), 5},
	}
	for _, pass := range passes {
		manager.logNodeStates(&pass.state)
		if logCount != pass.wantLogs {
			t.Fatalf("after %s: got %d summary logs, want %d", pass.name, logCount, pass.wantLogs)
		}
	}
}
