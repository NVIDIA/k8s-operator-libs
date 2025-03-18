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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
	"github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	DefaultNodeMaintenance maintenancev1alpha1.NodeMaintenance
)

func SetDefaultNodeMaintenance(opts RequestorOptions,
	upgradePolicy *v1alpha1.DriverUpgradePolicySpec) {
	drainSpec, podCompletion := convertV1Alpha1ToMaintenance(upgradePolicy, opts)
	DefaultNodeMaintenance = maintenancev1alpha1.NodeMaintenance{
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
	nm := DefaultNodeMaintenance
	nm.Name = nodeName
	nm.Spec.NodeName = nodeName

	return &nm
}

// CreateNodeMaintenance creates nodeMaintenance obj for designated node upgrade-required state
func (m *UpgradeManagerImpl) CreateNodeMaintenance(ctx context.Context, nodeState *base.NodeUpgradeState) error {
	nm := m.NewNodeMaintenance(nodeState.Node.Name)
	objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(nm)
	if err != nil {
		return fmt.Errorf("failed to convert maintenancev1alpha1.NodeMaintenance to unstructured: %v", err)
	}
	nodeState.NodeMaintenance = &unstructured.Unstructured{Object: objMap}
	m.Log.V(consts.LogLevelInfo).Info("creating node maintenance", nodeState.Node.Name, nm.Name)
	err = m.K8sClient.Create(ctx, nm, &client.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			m.Log.V(consts.LogLevelError).Error(err, "nodeMaintenance")
			return nil
		}
		return fmt.Errorf("failed to create node maintenance '%+v'. %v", nm, err)
	}

	return nil
}

// GetNodeMaintenance creates nodeMaintenance obj for designated node upgrade-required state
func (m *UpgradeManagerImpl) GetNodeMaintenance(ctx context.Context,
	nodeName string) (*unstructured.Unstructured, error) {
	nm := &maintenancev1alpha1.NodeMaintenance{}
	err := m.K8sClient.Get(ctx, types.NamespacedName{
		Name: nodeName, Namespace: m.opts.MaintenanceOPRequestorNS},
		nm, &client.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}
	objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(nm)
	if err != nil {
		return nil, fmt.Errorf("failed to convert maintenancev1alpha1.NodeMaintenance to unstructured: %v", err)
	}

	return &unstructured.Unstructured{Object: objMap}, nil
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

// TODO: Check if this is needed
func validateNodeMaintenance(nodeState *base.NodeUpgradeState) (*maintenancev1alpha1.NodeMaintenance, error) {
	if nodeState.NodeMaintenance == nil {
		return nil, fmt.Errorf("missing nodeMaintenance for specified nodeUpgradeState. %v", nodeState)
	}
	nm := &maintenancev1alpha1.NodeMaintenance{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(nodeState.NodeMaintenance.Object, nm)
	if err != nil {
		return nil, fmt.Errorf(`failed to convert NodeUpgradeState.NodeMaintenance unstructured obj
		 to maintenancev1alpha1.NodeMaintenance. %v`, nodeState)
	}
	return nm, nil
}
