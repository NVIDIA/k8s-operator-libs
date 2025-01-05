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

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/inplace"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/requestor"
)

// ExtendedUpgradeStateManager interface purpose is to decouple ApplyState implementation from base package
// since upgrade pkg is a high level abstaction referencing inplace/requestor (maintenance OP) packages
type ExtendedUpgradeStateManager interface {
	// ApplyState receives a complete cluster upgrade state and, based on upgrade policy, processes each node's state.
	// Based on the current state of the node, it is calculated if the node can be moved to the next state right now
	// or whether any actions need to be scheduled for the node to move to the next state.
	// The function is stateless and idempotent. If the error was returned before all nodes' states were processed,
	// ApplyState would be called again and complete the processing - all the decisions are based on the input data.
	ApplyState(ctx context.Context,
		currentState *base.ClusterUpgradeState, upgradePolicy *v1alpha1.DriverUpgradePolicySpec) (err error)
}

// ClusterUpgradeStateManager is an interface for performing cluster upgrades of driver containers
type ClusterUpgradeStateManager interface {
	ExtendedUpgradeStateManager
	base.CommonUpgradeStateManager
}

// ClusterUpgradeStateManagerImpl serves as a state machine for the ClusterUpgradeState
// It processes each node and based on its state schedules the required jobs to change their state to the next one
type ClusterUpgradeStateManagerImpl struct {
	*base.CommonUpgradeManagerImpl
	inplace   base.ProcessNodeStateManager
	requestor base.ProcessNodeStateManager
}

// NewClusterUpgradeStateManager creates a new instance of ClusterUpgradeStateManagerImpl
func NewClusterUpgradeStateManager(
	ctx context.Context,
	log logr.Logger,
	k8sConfig *rest.Config,
	eventRecorder record.EventRecorder) (ClusterUpgradeStateManager, error) {
	requestor.InitEnvs()

	common, err := base.NewCommonUpgradeStateManager(log, k8sConfig, requestor.Scheme, eventRecorder)
	if err != nil {
		return nil, fmt.Errorf("failed to create common upgrade state manager. %v", err)
	}
	request, err := requestor.NewRequestorUpgradeManagerImpl(ctx, k8sConfig, common)
	if err != nil && err != requestor.ErrNodeMaintenanceUpgradeDisabled {
		return nil, fmt.Errorf("failed to create requestor upgrade state manager. %v", err)
	}

	inplace, err := inplace.NewInplaceUpgradeManagerImpl(common)
	if err != nil {
		return nil, fmt.Errorf("failed to create inplace upgrade state manager. %v", err)
	}

	manager := &ClusterUpgradeStateManagerImpl{
		CommonUpgradeManagerImpl: common,
		requestor:                request,
		inplace:                  inplace,
	}

	return manager, nil
}

// ApplyState receives a complete cluster upgrade state and, based on upgrade policy, processes each node's state.
// Based on the current state of the node, it is calculated if the node can be moved to the next state right now
// or whether any actions need to be scheduled for the node to move to the next state.
// The function is stateless and idempotent. If the error was returned before all nodes' states were processed,
// ApplyState would be called again and complete the processing - all the decisions are based on the input data.
func (m *ClusterUpgradeStateManagerImpl) ApplyState(ctx context.Context,
	currentState *base.ClusterUpgradeState, upgradePolicy *v1alpha1.DriverUpgradePolicySpec) (err error) {
	m.Log.V(consts.LogLevelInfo).Info("State Manager, got state update")

	if currentState == nil {
		return fmt.Errorf("currentState should not be empty")
	}

	if upgradePolicy == nil || !upgradePolicy.AutoUpgrade {
		m.Log.V(consts.LogLevelInfo).Info("Driver auto upgrade is disabled, skipping")
		return nil
	}

	m.Log.V(consts.LogLevelInfo).Info("Node states:",
		"Unknown", len(currentState.NodeStates[base.UpgradeStateUnknown]),
		base.UpgradeStateDone, len(currentState.NodeStates[base.UpgradeStateDone]),
		base.UpgradeStateUpgradeRequired, len(currentState.NodeStates[base.UpgradeStateUpgradeRequired]),
		base.UpgradeStateCordonRequired, len(currentState.NodeStates[base.UpgradeStateCordonRequired]),
		base.UpgradeStateWaitForJobsRequired, len(currentState.NodeStates[base.UpgradeStateWaitForJobsRequired]),
		base.UpgradeStatePodDeletionRequired, len(currentState.NodeStates[base.UpgradeStatePodDeletionRequired]),
		base.UpgradeStateFailed, len(currentState.NodeStates[base.UpgradeStateFailed]),
		base.UpgradeStateDrainRequired, len(currentState.NodeStates[base.UpgradeStateDrainRequired]),
		base.UpgradeStatePostMaintenanceRequired, len(currentState.NodeStates[base.UpgradeStatePostMaintenanceRequired]),
		base.UpgradeStatePodRestartRequired, len(currentState.NodeStates[base.UpgradeStatePodRestartRequired]),
		base.UpgradeStateValidationRequired, len(currentState.NodeStates[base.UpgradeStateValidationRequired]),
		base.UpgradeStateUncordonRequired, len(currentState.NodeStates[base.UpgradeStateUncordonRequired]))

	// Determine the object to log this event
	// m.EventRecorder.Eventf(m.Namespace, v1.EventTypeNormal, GetEventReason(),
	// "InProgress: %d, MaxParallelUpgrades: %d, UpgradeSlotsAvailable: %s", upgradesInProgress,
	// upgradePolicy.MaxParallelUpgrades, upgradesAvailable)

	// First, check if unknown or ready nodes need to be upgraded
	err = m.ProcessDoneOrUnknownNodes(ctx, currentState, base.UpgradeStateUnknown)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to process nodes", "state", base.UpgradeStateUnknown)
		return err
	}
	err = m.ProcessDoneOrUnknownNodes(ctx, currentState, base.UpgradeStateDone)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to process nodes", "state", base.UpgradeStateDone)
		return err
	}
	// Start upgrade process for upgradesAvailable number of nodes
	if requestor.UseMaintenanceOperator {
		err = m.requestor.ProcessUpgradeRequiredNodes(ctx, currentState, upgradePolicy)
	} else {
		err = m.inplace.ProcessUpgradeRequiredNodes(ctx, currentState, upgradePolicy)
	}

	if err != nil {
		m.Log.V(consts.LogLevelError).Error(
			err, "Failed to process nodes", "state", base.UpgradeStateUpgradeRequired)
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

	if requestor.UseMaintenanceOperator {
		err = m.ProcessPostMaintenanceNodes(currentState)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "Failed to schedule pods restart")
			return err
		}
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

func (m *ClusterUpgradeStateManagerImpl) GetRequestor() base.ProcessNodeStateManager {
	return m.requestor
}
