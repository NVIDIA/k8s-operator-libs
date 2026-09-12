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
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// CordonManagerImpl implements CordonManager interface and can
// cordon / uncordon k8s nodes
type CordonManagerImpl struct {
	k8sInterface kubernetes.Interface
	log          logr.Logger
}

// CordonManager provides methods for cordoning / uncordoning nodes
type CordonManager interface {
	Cordon(ctx context.Context, node *corev1.Node) error
	Uncordon(ctx context.Context, node *corev1.Node) error
}

// ErrNodeCordonedExternally is returned by Cordon when the node is already cordoned by someone
// other than a GPU component.
var ErrNodeCordonedExternally = errors.New("node is cordoned by an external actor")

// gpuFieldManagers are the field managers whose writes to spec.unschedulable count as a GPU cordon.
// A component joining an existing cordon does not take over the field, so either manager is accepted.
var gpuFieldManagers = []string{UpgradeControllerFieldManager, DriverManagerFieldManager}

// Cordon marks a node as unschedulable
func (m *CordonManagerImpl) Cordon(ctx context.Context, node *corev1.Node) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := m.k8sInterface.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", node.Name, err)
		}

		if current.Spec.Unschedulable {
			// The node was cordoned before an upgrade that was allowed to proceed anyway.
			if current.Annotations[GetUpgradeInitialStateAnnotationKey()] == trueString {
				current.DeepCopyInto(node)
				return nil
			}
			if !hasGPUCordonClaim(current) {
				if current.Annotations[UpgradeControllerCordonClaimAnnotation] == trueString {
					current, err = m.patchNodeSchedulingState(ctx, current, true,
						map[string]any{UpgradeControllerCordonClaimAnnotation: nil})
					if err != nil {
						return err
					}
				}
				current.DeepCopyInto(node)
				return fmt.Errorf("%w: %s", ErrNodeCordonedExternally, node.Name)
			}
		}

		updated, err := m.patchNodeSchedulingState(ctx, current, true,
			map[string]any{UpgradeControllerCordonClaimAnnotation: trueString})
		if err != nil {
			return err
		}
		updated.DeepCopyInto(node)
		return nil
	})
}

// Uncordon marks a node as schedulable
func (m *CordonManagerImpl) Uncordon(ctx context.Context, node *corev1.Node) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := m.k8sInterface.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", node.Name, err)
		}

		claimed := current.Annotations[UpgradeControllerCordonClaimAnnotation] == trueString
		annotations := map[string]any{}
		if claimed {
			annotations[UpgradeControllerCordonClaimAnnotation] = nil
		}
		unschedulable := false
		switch {
		case !current.Spec.Unschedulable:
		case current.Annotations[GetUpgradeInitialStateAnnotationKey()] == trueString,
			driverManagerHoldsCordon(current),
			claimed && !upgradeControllerOwnsCordon(current):
			unschedulable = true
		default:
			// Last GPU component out. Controllers before the claim annotation cordoned without one.
			if current.Annotations[DriverManagerInitialUnschedulableAnnotation] == falseString {
				annotations[DriverManagerInitialUnschedulableAnnotation] = nil
			}
		}

		if len(annotations) == 0 && unschedulable == current.Spec.Unschedulable {
			current.DeepCopyInto(node)
			return nil
		}
		updated, err := m.patchNodeSchedulingState(ctx, current, unschedulable, annotations)
		if err != nil {
			return err
		}
		updated.DeepCopyInto(node)
		return nil
	})
}

func hasGPUCordonClaim(node *corev1.Node) bool {
	return driverManagerOwnsCordon(node) || upgradeControllerOwnsCordon(node)
}

func driverManagerOwnsCordon(node *corev1.Node) bool {
	marked := node.Annotations[DriverManagerCordonClaimAnnotation] == trueString ||
		node.Annotations[DriverManagerInitialUnschedulableAnnotation] == falseString
	return node.Spec.Unschedulable && marked && unschedulableManagedBy(node, gpuFieldManagers...)
}

func driverManagerHoldsCordon(node *corev1.Node) bool {
	return node.Spec.Unschedulable &&
		node.Annotations[DriverManagerCordonClaimAnnotation] == trueString &&
		unschedulableManagedBy(node, gpuFieldManagers...)
}

func upgradeControllerOwnsCordon(node *corev1.Node) bool {
	return node.Spec.Unschedulable &&
		node.Annotations[UpgradeControllerCordonClaimAnnotation] == trueString &&
		(IsNodeInRequestorMode(node) || unschedulableManagedBy(node, gpuFieldManagers...))
}

func unschedulableManagedBy(node *corev1.Node, managers ...string) bool {
	for _, entry := range node.ManagedFields {
		if entry.Subresource != "" || entry.FieldsV1 == nil || !slices.Contains(managers, entry.Manager) {
			continue
		}
		var fields struct {
			Spec map[string]json.RawMessage `json:"f:spec"`
		}
		if err := json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields); err != nil {
			continue
		}
		if _, ok := fields.Spec["f:unschedulable"]; ok {
			return true
		}
	}
	return false
}

func (m *CordonManagerImpl) patchNodeSchedulingState(
	ctx context.Context, node *corev1.Node, unschedulable bool, annotations map[string]any) (*corev1.Node, error) {
	patch := map[string]any{
		"metadata": map[string]any{
			"resourceVersion": node.ResourceVersion,
			"annotations":     annotations,
		},
		"spec": map[string]any{
			"unschedulable": unschedulable,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal node scheduling state patch: %w", err)
	}
	updated, err := m.k8sInterface.CoreV1().Nodes().Patch(
		ctx, node.Name, types.StrategicMergePatchType, patchBytes,
		metav1.PatchOptions{FieldManager: UpgradeControllerFieldManager})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// NewCordonManager returns a CordonManagerImpl
func NewCordonManager(k8sInterface kubernetes.Interface, log logr.Logger) *CordonManagerImpl {
	return &CordonManagerImpl{
		k8sInterface: k8sInterface,
		log:          log,
	}
}
