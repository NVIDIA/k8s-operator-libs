/*
Copyright 2025 NVIDIA CORPORATION & AFFILIATES

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

package base

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
)

// NodeUpgradeState contains a mapping between a node,
// the driver POD running on them and the daemon set, controlling this pod
type NodeUpgradeState struct {
	Node            *corev1.Node
	DriverPod       *corev1.Pod
	DriverDaemonSet *appsv1.DaemonSet
}

// IsOrphanedPod returns true if Pod is not associated to a DaemonSet
func (nus *NodeUpgradeState) IsOrphanedPod() bool {
	return nus.DriverDaemonSet == nil
}

// ClusterUpgradeState contains a snapshot of the driver upgrade state in the cluster
// It contains driver upgrade policy and mappings between nodes and their upgrade state
// Nodes are grouped together with the driver POD running on them and the daemon set, controlling this pod
// This state is then used as an input for the ClusterUpgradeStateManager
type ClusterUpgradeState struct {
	NodeStates map[string][]*NodeUpgradeState
}

// NewClusterUpgradeState creates an empty ClusterUpgradeState object
func NewClusterUpgradeState() ClusterUpgradeState {
	return ClusterUpgradeState{NodeStates: make(map[string][]*NodeUpgradeState)}
}

// ProcessNodeStateManager interface is used for abstracting both upgrade modes: inplace,
// requestor (e.g. maintenance OP)
// Similar node states are used in both modes, while changes are introduced within ApplyState Process<state>
// methods to support both modes logic
type ProcessNodeStateManager interface {
	ProcessUpgradeRequiredNodes(ctx context.Context,
		currentClusterState *ClusterUpgradeState, upgradePolicy *v1alpha1.DriverUpgradePolicySpec) error
	ProcessUncordonRequiredNodes(
		ctx context.Context, currentClusterState *ClusterUpgradeState) error
}
