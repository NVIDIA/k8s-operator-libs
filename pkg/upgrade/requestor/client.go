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
	"fmt"

	//nolint:depguard
	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	defaultNodeMaintenance *maintenancev1alpha1.NodeMaintenance
)

func SetDefaultNodeMaintenance(opts RequestorOptions,
	upgradePolicy *v1alpha1.DriverUpgradePolicySpec) {
	drainSpec, podCompletion := convertV1Alpha1ToMaintenance(upgradePolicy, opts)
	defaultNodeMaintenance = &maintenancev1alpha1.NodeMaintenance{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: opts.MaintenanceOPRequestorNS,
		},
		Spec: maintenancev1alpha1.NodeMaintenanceSpec{
			RequestorID:          opts.MaintenanceOPRequestorID,
			WaitForPodCompletion: podCompletion,
			DrainSpec:            drainSpec,
		},
	}
}

func (m *UpgradeManagerImpl) NewNodeMaintenance(nodeName string) *maintenancev1alpha1.NodeMaintenance {
	nm := defaultNodeMaintenance.DeepCopy()
	nm.Name = nodeName
	nm.Spec.NodeName = nodeName

	return nm
}

// CreateNodeMaintenance creates nodeMaintenance obj for designated node upgrade-required state
func (m *UpgradeManagerImpl) CreateNodeMaintenance(ctx context.Context, nodeState *base.NodeUpgradeState) error {
	nm := m.NewNodeMaintenance(nodeState.Node.Name)
	nodeState.NodeMaintenance = nm
	m.Log.V(consts.LogLevelInfo).Info("creating node maintenance", nodeState.Node.Name, nm.Name)
	err := m.K8sClient.Create(ctx, nm, &client.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			m.Log.V(consts.LogLevelWarning).Info("nodeMaintenance", nm.Name, "already exists")
			return nil
		}
		return fmt.Errorf("failed to create node maintenance '%+v'. %v", nm, err)
	}

	return nil
}

// GetNodeMaintenanceObj creates nodeMaintenance obj for designated node upgrade-required state
func (m *UpgradeManagerImpl) GetNodeMaintenanceObj(ctx context.Context,
	nodeName string) (client.Object, error) {
	nm := &maintenancev1alpha1.NodeMaintenance{}
	err := m.K8sClient.Get(ctx, types.NamespacedName{
		Name: nodeName, Namespace: m.opts.MaintenanceOPRequestorNS},
		nm, &client.GetOptions{})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return nil, err
		}
		// explicitly return nil so returned interface is truly nil
		return nil, nil
	}
	return nm, nil
}

// DeleteNodeMaintenance requests to delete nodeMaintenance obj
func (m *UpgradeManagerImpl) DeleteNodeMaintenance(ctx context.Context, nodeState *base.NodeUpgradeState) error {
	_, err := validateNodeMaintenance(nodeState)
	if err != nil {
		return err
	}
	nm := &maintenancev1alpha1.NodeMaintenance{}
	err = m.K8sClient.Get(ctx, types.NamespacedName{Name: nodeState.Node.Name,
		Namespace: m.opts.MaintenanceOPRequestorNS},
		nm, &client.GetOptions{})
	if err != nil {
		return err
	}
	// send deletion request assuming maintenance OP will handle actual obj deletion
	err = m.K8sClient.Delete(ctx, nm)
	if err != nil {
		return err
	}
	return nil
}

func validateNodeMaintenance(nodeState *base.NodeUpgradeState) (*maintenancev1alpha1.NodeMaintenance, error) {
	if nodeState.NodeMaintenance == nil {
		return nil, fmt.Errorf("missing nodeMaintenance for specified nodeUpgradeState. %v", nodeState)
	}
	nm, ok := nodeState.NodeMaintenance.(*maintenancev1alpha1.NodeMaintenance)
	if !ok {
		return nil, fmt.Errorf("failed to cast object to NodeMaintenance. %v", nodeState.NodeMaintenance)
	}
	return nm, nil
}
