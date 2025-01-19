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
	"errors"
	"fmt"
	"sync"

	//nolint:depguard
	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
)

var (
	// UseMaintenanceOperator enables requestor updrade mode
	UseMaintenanceOperator            bool
	MaintenanceOPRequestorID          = "nvidia.operator.com"
	MaintenanceOPFinalizerName        = "maintenance.nvidia.com/finalizer"
	MaintenanceOPRequestorNS          string
	MaintenanceOPPodEvictionFilter    *string
	MaintenanceOPControllerName       = "node-maintenance"
	MaintenanceOPRequestorCordon      = true // TODO: do we need to set this dynamically
	ErrNodeMaintenanceUpgradeDisabled = errors.New("node maintenance upgrade mode is disabled")
)

// UpgradeManagerImpl contains concrete implementations for distinct requestor
// (e.g. maintenance OP) upgrade mode
type UpgradeManagerImpl struct {
	*base.CommonUpgradeManagerImpl
	Ctrl       manager.Manager
	Reconciler *NodeMaintenanceReconciler
	Wg         *sync.WaitGroup
}

// NewClusterUpgradeStateManager creates a new instance of UpgradeManagerImpl
func NewRequestorUpgradeManagerImpl(
	ctx context.Context,
	k8sConfig *rest.Config,
	common *base.CommonUpgradeManagerImpl) (base.ProcessNodeStateManager, error) {
	if !UseMaintenanceOperator {
		common.Log.V(consts.LogLevelInfo).Info("node maintenance upgrade mode is disabled")
		return nil, ErrNodeMaintenanceUpgradeDisabled
	}
	ctr, err := ctrl.NewManager(k8sConfig, ctrl.Options{
		Scheme:         Scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
	})
	if err != nil {
		common.Log.V(consts.LogLevelError).Error(err, "unable to create manager")
		return nil, err
	}
	reconciler := &NodeMaintenanceReconciler{
		Client:   common.K8sClient,
		Scheme:   ctr.GetScheme(),
		StatusCh: make(chan NodeMaintenanceCondition, 1),
	}
	if err := reconciler.SetupWithManager(ctr, common.Log); err != nil {
		common.Log.V(consts.LogLevelError).Error(err, "unable to create controller", "controller", "NodeMaintenance")
		return nil, err
	}
	manager := &UpgradeManagerImpl{
		CommonUpgradeManagerImpl: common,
		Ctrl:                     ctr,
		Reconciler:               reconciler,
		Wg:                       &sync.WaitGroup{},
	}
	// Starting nodeMaintenance reconciler for watching status change
	manager.Start(ctx)

	return manager, nil
}

func (m *UpgradeManagerImpl) Start(ctx context.Context) {
	m.Wg.Add(1)
	go func() {
		defer m.Wg.Done()
		err := m.Ctrl.Start(ctx)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "failed controller manager")
		}
	}()

	go func() {
		err := m.WatchNodeMaintenanceConditionChange(ctx)
		if err != nil {
			m.Log.V(consts.LogLevelWarning).Error(err, "failed to update node condition")
		}
	}()
}

// ProcessUpgradeRequiredNodes processes UpgradeStateUpgradeRequired nodes and moves them to UpgradeStateCordonRequired
// until the limit on max parallel upgrades is reached.
func (m *UpgradeManagerImpl) ProcessUpgradeRequiredNodes(
	ctx context.Context, currentClusterState *base.ClusterUpgradeState,
	upgradePolicy *v1alpha1.DriverUpgradePolicySpec) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessUpgradeRequiredNodes")

	SetDefaultNodeMaintenance(MaintenanceOPRequestorCordon, upgradePolicy)
	for _, nodeState := range currentClusterState.NodeStates[base.UpgradeStateUpgradeRequired] {
		err := m.CreateNodeMaintenance(ctx, nodeState)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "failed to create nodeMaintenance")
			return err
		}
		// update node state to 'node-maintenance-required'
		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, base.UpgradeStateNodeMaintenanceRequired)
		if err != nil {
			return fmt.Errorf("failed to update node state. %v", err)
		}
	}

	return nil
}

func (m *UpgradeManagerImpl) ProcessUncordonRequiredNodes(
	ctx context.Context, currentClusterState *base.ClusterUpgradeState) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessUncordonRequiredNodes")

	for _, nodeState := range currentClusterState.NodeStates[base.UpgradeStateUncordonRequired] {
		m.Log.V(consts.LogLevelDebug).Info("deleting node maintenance",
			nodeState.NodeMaintenance.GetName(), nodeState.NodeMaintenance.GetNamespace())
		err := m.DeleteNodeMaintenance(ctx, nodeState)
		if err != nil {
			m.Log.V(consts.LogLevelWarning).Error(
				err, "Node uncordon failed", "node", nodeState.Node)
			return err
		}
	}
	return nil
}

// WatchNodeMaintenanceConditionChange waits for nodeMaintenance status change by reconciler
// in case of status change, fetch referenced node and update post maintenance state
func (m *UpgradeManagerImpl) WatchNodeMaintenanceConditionChange(ctx context.Context) error {
	m.Log.V(consts.LogLevelInfo).Info("starting node-maintenance condition change watcher")
	for {
		select {
		case <-ctx.Done():
			close(m.Reconciler.StatusCh)
			m.Wg.Wait()
			m.Log.V(consts.LogLevelInfo).Info("terminated controller manager")
			return ctx.Err()
		case cond, ok := <-m.Reconciler.StatusCh:
			if !ok {
				// Channel is closed
				return nil
			}
			node, err := m.NodeUpgradeStateProvider.GetNode(ctx, cond.NodeName)
			if err != nil {
				m.Log.V(consts.LogLevelError).Error(err, "failed to find node")
				continue
			}
			if cond.Reason == "deleting" {
				//TODO: add handler func handleNodeMaintenanceDeletion(node)
				upgradeStateLabel := base.GetUpgradeStateLabelKey()
				m.Log.V(consts.LogLevelInfo).Info("handle NodeMaintenance deletion", node.Name, node.Labels[upgradeStateLabel])
				if node.Labels[upgradeStateLabel] == base.UpgradeStateUncordonRequired {
					err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, base.UpgradeStateDone)
					if err != nil {
						m.Log.V(consts.LogLevelError).Error(err, "failed to update node state")
						continue
					}
				}
			}
			// verify node maintenance operation completed
			// node should enter post maintenance state
			if cond.Reason == maintenancev1alpha1.ConditionReasonReady {
				m.Log.V(consts.LogLevelDebug).Info("handle NodeMaintenance update", node.Name, cond.NodeName)
				// update node state to 'post-maintenance-required'
				err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, base.UpgradeStatePostMaintenanceRequired)
				if err != nil {
					m.Log.V(consts.LogLevelError).Error(err, "failed to update node state")
				}
			}
		}
	}
}

// convertV1Alpha1ToMaintenance explicitly converts v1alpha1.DriverUpgradePolicySpec
// to maintenancev1alpha1.DrainSpec and maintenancev1alpha1.WaitForPodCompletionSpec and
func convertV1Alpha1ToMaintenance(upgradePolicy *v1alpha1.DriverUpgradePolicySpec) (*maintenancev1alpha1.DrainSpec,
	*maintenancev1alpha1.WaitForPodCompletionSpec) {
	if upgradePolicy == nil {
		return nil, nil
	}
	drainSpec := &maintenancev1alpha1.DrainSpec{}
	if upgradePolicy.DrainSpec != nil {
		drainSpec.Force = upgradePolicy.DrainSpec.Force
		drainSpec.PodSelector = upgradePolicy.DrainSpec.PodSelector
		//nolint:gosec // G115: suppress potential integer overflow conversion warning
		drainSpec.TimeoutSecond = int32(upgradePolicy.DrainSpec.TimeoutSecond)
		drainSpec.DeleteEmptyDir = upgradePolicy.DrainSpec.DeleteEmptyDir
	}
	if upgradePolicy.PodDeletion != nil {
		//TODO: propagate
		drainSpec.PodEvictionFilters = nil
	}
	podComplition := &maintenancev1alpha1.WaitForPodCompletionSpec{}
	if upgradePolicy.WaitForCompletion != nil {
		podComplition.PodSelector = upgradePolicy.WaitForCompletion.PodSelector
		//nolint:gosec // G115: suppress potential integer overflow conversion warning
		podComplition.TimeoutSecond = int32(upgradePolicy.WaitForCompletion.TimeoutSecond)
	}

	return drainSpec, podComplition
}
