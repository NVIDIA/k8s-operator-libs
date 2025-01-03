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

package requestor

import (
	"context"

	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
)

// UpgradeManagerImpl contains concrete implementations for distinct requestor
// (e.g. maintenance OP) upgrade mode
type UpgradeManagerImpl struct {
	*base.CommonUpgradeManagerImpl
}

// ProcessUpgradeRequiredNodes processes UpgradeStateUpgradeRequired nodes and moves them to UpgradeStateCordonRequired
// until the limit on max parallel upgrades is reached.
func (m *UpgradeManagerImpl) ProcessUpgradeRequiredNodes(
	ctx context.Context, currentClusterState *base.ClusterUpgradeState, upgradesAvailable int) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessUpgradeRequiredNodes")
	for _, nodeState := range currentClusterState.NodeStates[base.UpgradeStateUpgradeRequired] {
		if m.IsUpgradeRequested(nodeState.Node) {
			// Make sure to remove the upgrade-requested annotation
			err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeAnnotation(ctx, nodeState.Node,
				base.GetUpgradeRequestedAnnotationKey(), "null")
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

		err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, base.UpgradeStateCordonRequired)
		if err == nil {
			upgradesAvailable--
			m.Log.V(consts.LogLevelInfo).Info("Node waiting for cordon",
				"node", nodeState.Node.Name)
		} else {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", base.UpgradeStateCordonRequired)
			return err
		}
	}

	return nil
}
