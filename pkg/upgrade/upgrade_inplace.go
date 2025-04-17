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

package upgrade

import (
	"context"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
)

// InplaceUpgradeManagerImpl contains concrete implementations for distinct inplace upgrade mode
type InplaceUpgradeManagerImpl struct {
	*CommonUpgradeManagerImpl
}

// NewClusterUpgradeStateManager creates a new instance of InplaceUpgradeManagerImpl
func NewInplaceUpgradeManagerImpl(commonmanager *CommonUpgradeManagerImpl) (ProcessNodeStateManager,
	error) {
	manager := &InplaceUpgradeManagerImpl{
		CommonUpgradeManagerImpl: commonmanager,
	}
	return manager, nil
}

// ProcessUpgradeRequiredNodes processes UpgradeStateUpgradeRequired nodes and moves them to UpgradeStateCordonRequired
// until the limit on max parallel upgrades is reached.
func (m *InplaceUpgradeManagerImpl) ProcessUpgradeRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
	upgradePolicy *v1alpha1.DriverUpgradePolicySpec) error {
	var err error

	totalNodes := m.GetTotalManagedNodes(currentClusterState)
	upgradesInProgress := m.GetUpgradesInProgress(currentClusterState)
	currentUnavailableNodes := m.GetCurrentUnavailableNodes(currentClusterState)
	maxUnavailable := totalNodes

	if upgradePolicy.MaxUnavailable != nil {
		maxUnavailable, err = intstr.GetScaledValueFromIntOrPercent(upgradePolicy.MaxUnavailable, totalNodes, true)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "Failed to compute maxUnavailable from the current total nodes")
			return err
		}
	}
	upgradesAvailable := m.GetUpgradesAvailable(currentClusterState, upgradePolicy.MaxParallelUpgrades,
		maxUnavailable)
	m.Log.V(consts.LogLevelInfo).Info("Upgrades in progress",
		"currently in progress", upgradesInProgress,
		"max parallel upgrades", upgradePolicy.MaxParallelUpgrades,
		"upgrade slots available", upgradesAvailable,
		"currently unavailable nodes", currentUnavailableNodes,
		"total number of nodes", totalNodes,
		"maximum nodes that can be unavailable", maxUnavailable)

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateUpgradeRequired] {
		if m.IsUpgradeRequested(nodeState.Node) {
			// Make sure to remove the upgrade-requested annotation
			err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeAnnotation(ctx, nodeState.Node,
				GetUpgradeRequestedAnnotationKey(), "null")
			if err != nil {
				m.Log.V(consts.LogLevelError).Error(
					err, "Failed to delete node upgrade-requested annotation")
				return err
			}
		}
		if m.SkipNodeUpgrade(nodeState.Node) {
			m.Log.V(consts.LogLevelInfo).Info("Node is marked for skipping upgrades", "node", nodeState.Node.Name)
			continue
		}

		if upgradesAvailable <= 0 {
			// when no new node upgrades are available, progess with manually cordoned nodes
			if m.IsNodeUnschedulable(nodeState.Node) {
				m.Log.V(consts.LogLevelDebug).Info("Node is already cordoned, progressing for driver upgrade",
					"node", nodeState.Node.Name)
			} else {
				m.Log.V(consts.LogLevelDebug).Info("Node upgrade limit reached, pausing further upgrades",
					"node", nodeState.Node.Name)
				continue
			}
		}

		err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateCordonRequired)
		if err == nil {
			upgradesAvailable--
			m.Log.V(consts.LogLevelInfo).Info("Node waiting for cordon",
				"node", nodeState.Node.Name)
		} else {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateCordonRequired)
			return err
		}
	}

	return nil
}

func (m *InplaceUpgradeManagerImpl) ProcessNodeMaintenanceRequiredNodes(ctx context.Context,
	currentClusterState *ClusterUpgradeState) error {
	// TODO: in future versions we'll remove 'pod-restart-required' and use 'post-maintenance-required' instead
	return m.ProcessPodRestartNodes(ctx, currentClusterState)
}

// ProcessUncordonRequiredNodes processes UpgradeStateUncordonRequired nodes,
// uncordons them and moves them to UpgradeStateDone state
func (m *InplaceUpgradeManagerImpl) ProcessUncordonRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessUncordonRequiredNodes")

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateUncordonRequired] {
		// skip in case node had undergone uncordon by maintenance operator
		if nodeState.NodeMaintenance != nil {
			continue
		}
		err := m.CordonManager.Uncordon(ctx, nodeState.Node)
		if err != nil {
			m.Log.V(consts.LogLevelWarning).Error(
				err, "Node uncordon failed", "node", nodeState.Node)
			return err
		}
		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateDone)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateDone)
			return err
		}
	}
	return nil
}
