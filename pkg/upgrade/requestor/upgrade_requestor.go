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

package requestor

import (
	"context"
	"errors"
	"fmt"

	//nolint:depguard
	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base/processor"
)

const (
	MaintenanceOPFinalizerName      = "maintenance.nvidia.com/finalizer"
	MaintenanceOPDefaultRequestorID = "nvidia.network.operator"
	MaintenanceOPEvictionGPU        = "nvidia.com/gpu-*"
	MaintenanceOPEvictionRDMA       = "nvidia.com/rdma*"
)

var (
	ErrNodeMaintenanceUpgradeDisabled = errors.New("node maintenance upgrade mode is disabled")
)

//nolint:revive
type RequestorOptions struct {
	// UseMaintenanceOperator enables requestor updrade mode
	UseMaintenanceOperator         bool
	MaintenanceOPRequestorID       string
	MaintenanceOPRequestorNS       string
	MaintenanceOPPodEvictionFilter []maintenancev1alpha1.PodEvictionFiterEntry
}

// UpgradeManagerImpl contains concrete implementations for distinct requestor
// (e.g. maintenance OP) upgrade mode
type UpgradeManagerImpl struct {
	*processor.CommonUpgradeManagerImpl
	opts RequestorOptions
}

// NewClusterUpgradeStateManager creates a new instance of UpgradeManagerImpl
func NewRequestorUpgradeManagerImpl(
	common *processor.CommonUpgradeManagerImpl,
	opts RequestorOptions) (base.ProcessNodeStateManager, error) {
	if !opts.UseMaintenanceOperator {
		common.Log.V(consts.LogLevelInfo).Info("node maintenance upgrade mode is disabled")
		return nil, ErrNodeMaintenanceUpgradeDisabled
	}
	manager := &UpgradeManagerImpl{
		opts:                     opts,
		CommonUpgradeManagerImpl: common,
	}

	return manager, nil
}

// ProcessUpgradeRequiredNodes processes UpgradeStateUpgradeRequired nodes and moves them to UpgradeStateCordonRequired
// until the limit on max parallel upgrades is reached.
func (m *UpgradeManagerImpl) ProcessUpgradeRequiredNodes(
	ctx context.Context, currentClusterState *base.ClusterUpgradeState,
	upgradePolicy *v1alpha1.DriverUpgradePolicySpec) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessUpgradeRequiredNodes")

	SetDefaultNodeMaintenance(m.opts, upgradePolicy)
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

		err := m.CreateNodeMaintenance(ctx, nodeState)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "failed to create nodeMaintenance")
			return err
		}

		err = m.SetUpgradeRequestorModeAnnotation(ctx, nodeState.Node.Name)
		if err != nil {
			return fmt.Errorf("failed annotate node for 'upgrade-requestor-mode'. %v", err)
		}
		// update node state to 'node-maintenance-required'
		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node,
			base.UpgradeStateNodeMaintenanceRequired)
		if err != nil {
			return fmt.Errorf("failed to update node state. %v", err)
		}
	}

	return nil
}

// ProcessPostMaintenanceNodes processes UpgradeStatePostMaintenanceRequired
// by adding UpgradeStatePodRestartRequired under existing UpgradeStatePodRestartRequired nodes list.
// the motivation is later to replace ProcessPodRestartNodes to a generic post node operation
// while using maintenance operator (e.g. post-maintenance-required)
func (m *UpgradeManagerImpl) ProcessPostMaintenanceNodes(ctx context.Context,
	currentClusterState *base.ClusterUpgradeState) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessPostMaintenanceNodes")
	for _, nodeState := range currentClusterState.NodeStates[base.UpgradeStateNodeMaintenanceRequired] {
		if nodeState.NodeMaintenance == nil {
			if _, ok := nodeState.Node.Annotations[base.GetUpgradeRequestorModeAnnotationKey()]; !ok {
				m.Log.V(consts.LogLevelWarning).Info("missing node annotation", "node", nodeState.Node.Name,
					"annotations", nodeState.Node.Annotations)
			}
			continue
		}
		nm := &maintenancev1alpha1.NodeMaintenance{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(nodeState.NodeMaintenance.Object, nm)
		if err != nil {
			return fmt.Errorf("failed to convert node mantenance obj. %v", err)
		}
		cond := meta.FindStatusCondition(nm.Status.Conditions, maintenancev1alpha1.ConditionReasonReady)
		if cond != nil {
			if cond.Reason == maintenancev1alpha1.ConditionReasonReady {
				m.Log.V(consts.LogLevelDebug).Info("node maintenance operation completed", nm.Spec.NodeName, cond.Reason)
				// update node state to 'pod-restart-required'
				err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node,
					base.UpgradeStatePodRestartRequired)
				if err != nil {
					return fmt.Errorf("failed to update node state. %v", err)
				}
			}
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
			if !k8serrors.IsNotFound(err) {
				m.Log.V(consts.LogLevelWarning).Error(
					err, "Node uncordon failed", "node", nodeState.Node)
				return err
			}
			// this means that node maintenance obj has been deleted
			err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node,
				base.UpgradeStateDone)
			if err != nil {
				return fmt.Errorf("failed to update node state. %v", err)
			}
			// remove requestor upgrade annotation
			err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeAnnotation(ctx,
				nodeState.Node, base.GetUpgradeRequestorModeAnnotationKey(), "null")
			if err != nil {
				return fmt.Errorf("failed to remove '%s' annotation . %v", base.GetUpgradeRequestorModeAnnotationKey(), err)
			}
		}
	}
	return nil
}

// SetUpgradeRequestorModeAnnotation will set upgrade-requestor-mode for specifying node upgrade mode flow
func (m *UpgradeManagerImpl) SetUpgradeRequestorModeAnnotation(ctx context.Context, nodeName string) error {
	node := &corev1.Node{}
	nodeKey := client.ObjectKey{
		Name: nodeName,
	}
	if err := m.K8sClient.Get(ctx, nodeKey, node); err != nil {
		return err
	}
	patchString := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q: "true"}}}`,
		base.GetUpgradeRequestorModeAnnotationKey()))
	patch := client.RawPatch(types.MergePatchType, patchString)
	err := m.K8sClient.Patch(ctx, node, patch)
	if err != nil {
		return err
	}
	m.Log.V(consts.LogLevelDebug).Info("Node annotated with upgrade-requestor-mode", "name", nodeName)

	return nil
}

// convertV1Alpha1ToMaintenance explicitly converts v1alpha1.DriverUpgradePolicySpec
// to maintenancev1alpha1.DrainSpec and maintenancev1alpha1.WaitForPodCompletionSpec and
func convertV1Alpha1ToMaintenance(upgradePolicy *v1alpha1.DriverUpgradePolicySpec,
	opts RequestorOptions) (*maintenancev1alpha1.DrainSpec,
	*maintenancev1alpha1.WaitForPodCompletionSpec) {
	var podComplition *maintenancev1alpha1.WaitForPodCompletionSpec
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
		drainSpec.PodEvictionFilters = opts.MaintenanceOPPodEvictionFilter
	}
	if upgradePolicy.WaitForCompletion != nil {
		podComplition = &maintenancev1alpha1.WaitForPodCompletionSpec{
			PodSelector: upgradePolicy.WaitForCompletion.PodSelector,
			//nolint:gosec // G115: suppress potential integer overflow conversion warning
			TimeoutSecond: int32(upgradePolicy.WaitForCompletion.TimeoutSecond),
		}
	}

	return drainSpec, podComplition
}
