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

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/common"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/inbox"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/requestor"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
)

// ExtendedUpgradeStateManager interface purpose is to decouple ApplyState implementation from common package
// since its referencing inbox, requestor (maintenance OP) packages
type ExtendedUpgradeStateManager interface {
	// ApplyState receives a complete cluster upgrade state and, based on upgrade policy, processes each node's state.
	// Based on the current state of the node, it is calculated if the node can be moved to the next state right now
	// or whether any actions need to be scheduled for the node to move to the next state.
	// The function is stateless and idempotent. If the error was returned before all nodes' states were processed,
	// ApplyState would be called again and complete the processing - all the decisions are based on the input data.
	ApplyState(ctx context.Context,
		currentState *common.ClusterUpgradeState, upgradePolicy *v1alpha1.DriverUpgradePolicySpec) (err error)
}

// ProcessNodeStateManager interface is used for abstracting both upgrade modes: inbox, requestor (e.g. maintenance OP)
// Similar node states are used in both modes, while changes are introduced within ApplyState Process<state> methods to
// support both modes logic
type ProcessNodeStateManager interface {
	ProcessUpgradeRequiredNodes(ctx context.Context, currentClusterState *common.ClusterUpgradeState, upgradesAvailable int) error
}

// ClusterUpgradeStateManager is an interface for performing cluster upgrades of driver containers
//
//nolint:interfacebloat
type ClusterUpgradeStateManager interface {
	ExtendedUpgradeStateManager
	common.CommonUpgradeStateManager
}

// ClusterUpgradeStateManagerImpl serves as a state machine for the ClusterUpgradeState
// It processes each node and based on its state schedules the required jobs to change their state to the next one
type ClusterUpgradeStateManagerImpl struct {
	*common.CommonUpgradeManagerImpl
	inbox     ProcessNodeStateManager
	requestor ProcessNodeStateManager
}

// NewClusterUpgradeStateManager creates a new instance of RequestorUpgradeManagerImpl
func NewRequestorUpgradeManagerImpl(
	common *common.CommonUpgradeManagerImpl) (ProcessNodeStateManager, error) {

	manager := &requestor.RequestorUpgradeManagerImpl{
		CommonUpgradeManagerImpl: common,
	}
	return manager, nil
}

// NewClusterUpgradeStateManager creates a new instance of InboxUpgradeManagerImpl
func NewInboxUpgradeManagerImpl(
	common *common.CommonUpgradeManagerImpl) (ProcessNodeStateManager, error) {

	manager := &inbox.InboxUpgradeManagerImpl{
		CommonUpgradeManagerImpl: common,
	}
	return manager, nil
}

// NewClusterUpgradeStateManager creates a new instance of ClusterUpgradeStateManagerImpl
func NewClusterUpgradeStateManager(
	log logr.Logger,
	k8sConfig *rest.Config,
	eventRecorder record.EventRecorder) (ClusterUpgradeStateManager, error) {

	common, _ := common.NewCommonUpgradeStateManager(log, k8sConfig, eventRecorder)
	request, _ := NewRequestorUpgradeManagerImpl(common)
	inbox, _ := NewInboxUpgradeManagerImpl(common)

	manager := &ClusterUpgradeStateManagerImpl{
		CommonUpgradeManagerImpl: common,
		requestor:                request,
		inbox:                    inbox,
	}

	return manager, nil
}

// ApplyState receives a complete cluster upgrade state and, based on upgrade policy, processes each node's state.
// Based on the current state of the node, it is calculated if the node can be moved to the next state right now
// or whether any actions need to be scheduled for the node to move to the next state.
// The function is stateless and idempotent. If the error was returned before all nodes' states were processed,
// ApplyState would be called again and complete the processing - all the decisions are based on the input data.
//
//nolint:funlen
func (m *ClusterUpgradeStateManagerImpl) ApplyState(ctx context.Context,
	currentState *common.ClusterUpgradeState, upgradePolicy *v1alpha1.DriverUpgradePolicySpec) (err error) {
	m.Log.V(consts.LogLevelInfo).Info("State Manager, got state update")

	if currentState == nil {
		return fmt.Errorf("currentState should not be empty")
	}

	if upgradePolicy == nil || !upgradePolicy.AutoUpgrade {
		m.Log.V(consts.LogLevelInfo).Info("Driver auto upgrade is disabled, skipping")
		return nil
	}

	m.Log.V(consts.LogLevelInfo).Info("Node states:",
		"Unknown", len(currentState.NodeStates[common.UpgradeStateUnknown]),
		common.UpgradeStateDone, len(currentState.NodeStates[common.UpgradeStateDone]),
		common.UpgradeStateUpgradeRequired, len(currentState.NodeStates[common.UpgradeStateUpgradeRequired]),
		common.UpgradeStateCordonRequired, len(currentState.NodeStates[common.UpgradeStateCordonRequired]),
		common.UpgradeStateWaitForJobsRequired, len(currentState.NodeStates[common.UpgradeStateWaitForJobsRequired]),
		common.UpgradeStatePodDeletionRequired, len(currentState.NodeStates[common.UpgradeStatePodDeletionRequired]),
		common.UpgradeStateFailed, len(currentState.NodeStates[common.UpgradeStateFailed]),
		common.UpgradeStateDrainRequired, len(currentState.NodeStates[common.UpgradeStateDrainRequired]),
		common.UpgradeStatePodRestartRequired, len(currentState.NodeStates[common.UpgradeStatePodRestartRequired]),
		common.UpgradeStateValidationRequired, len(currentState.NodeStates[common.UpgradeStateValidationRequired]),
		common.UpgradeStateUncordonRequired, len(currentState.NodeStates[common.UpgradeStateUncordonRequired]))

	totalNodes := m.GetTotalManagedNodes(ctx, currentState)
	upgradesInProgress := m.GetUpgradesInProgress(ctx, currentState)
	currentUnavailableNodes := m.GetCurrentUnavailableNodes(ctx, currentState)
	maxUnavailable := totalNodes

	if upgradePolicy.MaxUnavailable != nil {
		maxUnavailable, err = intstr.GetScaledValueFromIntOrPercent(upgradePolicy.MaxUnavailable, totalNodes, true)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "Failed to compute maxUnavailable from the current total nodes")
			return err
		}
	}

	upgradesAvailable := m.GetUpgradesAvailable(ctx, currentState, upgradePolicy.MaxParallelUpgrades, maxUnavailable)

	m.Log.V(consts.LogLevelInfo).Info("Upgrades in progress",
		"currently in progress", upgradesInProgress,
		"max parallel upgrades", upgradePolicy.MaxParallelUpgrades,
		"upgrade slots available", upgradesAvailable,
		"currently unavailable nodes", currentUnavailableNodes,
		"total number of nodes", totalNodes,
		"maximum nodes that can be unavailable", maxUnavailable)

	// Determine the object to log this event
	// m.EventRecorder.Eventf(m.Namespace, v1.EventTypeNormal, GetEventReason(),
	// "InProgress: %d, MaxParallelUpgrades: %d, UpgradeSlotsAvailable: %s", upgradesInProgress,
	// upgradePolicy.MaxParallelUpgrades, upgradesAvailable)

	// First, check if unknown or ready nodes need to be upgraded
	err = m.ProcessDoneOrUnknownNodes(ctx, currentState, common.UpgradeStateUnknown)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to process nodes", "state", common.UpgradeStateUnknown)
		return err
	}
	err = m.ProcessDoneOrUnknownNodes(ctx, currentState, common.UpgradeStateDone)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to process nodes", "state", common.UpgradeStateDone)
		return err
	}
	// Start upgrade process for upgradesAvailable number of nodes
	err = m.inbox.ProcessUpgradeRequiredNodes(ctx, currentState, upgradesAvailable)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(
			err, "Failed to process nodes", "state", common.UpgradeStateUpgradeRequired)
		return err
	}

	err = m.ProcessCordonRequiredNodes(ctx, currentState)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to cordon nodes")
		return err
	}

	err = m.ProcessWaitForJobsRequiredNodes(ctx, currentState, upgradePolicy.WaitForCompletion)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to waiting for required jobs to complete")
		return err
	}

	drainEnabled := upgradePolicy.DrainSpec != nil && upgradePolicy.DrainSpec.Enable
	err = m.ProcessPodDeletionRequiredNodes(ctx, currentState, upgradePolicy.PodDeletion, drainEnabled)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to delete pods")
		return err
	}

	// Schedule nodes for drain
	err = m.ProcessDrainNodes(ctx, currentState, upgradePolicy.DrainSpec)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to schedule nodes drain")
		return err
	}
	err = m.ProcessPodRestartNodes(ctx, currentState)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to schedule pods restart")
		return err
	}
	err = m.ProcessUpgradeFailedNodes(ctx, currentState)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to process nodes in 'upgrade-failed' state")
		return err
	}
	err = m.ProcessValidationRequiredNodes(ctx, currentState)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to validate driver upgrade")
		return err
	}
	err = m.ProcessUncordonRequiredNodes(ctx, currentState)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to uncordon nodes")
		return err
	}
	m.Log.V(consts.LogLevelInfo).Info("State Manager, finished processing")
	return nil
}