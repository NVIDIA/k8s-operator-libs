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
	"fmt"

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

// Cordon marks a node as unschedulable
func (m *CordonManagerImpl) Cordon(ctx context.Context, node *corev1.Node) error {
	return m.updateCordonClaim(ctx, node, true)
}

// Uncordon marks a node as schedulable
func (m *CordonManagerImpl) Uncordon(ctx context.Context, node *corev1.Node) error {
	return m.updateCordonClaim(ctx, node, false)
}

// updateCordonClaim updates the upgrade-controller cordon claim and node
// schedulability in one conflict-retried strategic-merge patch.
func (m *CordonManagerImpl) updateCordonClaim(ctx context.Context, node *corev1.Node, acquire bool) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := m.k8sInterface.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", node.Name, err)
		}

		annotations := map[string]any{}
		var unschedulable bool

		if acquire {
			// Do not claim an external cordon recorded as the node's initial state.
			if current.Spec.Unschedulable &&
				current.Annotations[GetUpgradeInitialStateAnnotationKey()] == trueString {
				current.DeepCopyInto(node)
				return nil
			}
			annotations[UpgradeControllerCordonClaimAnnotation] = trueString
			unschedulable = true
		} else {
			if current.Annotations[UpgradeControllerCordonClaimAnnotation] != trueString {
				current.DeepCopyInto(node)
				return nil
			}
			annotations[UpgradeControllerCordonClaimAnnotation] = nil
			_, dmInitialState := current.Annotations[DriverManagerInitialUnschedulableAnnotation]
			_, initiallyUnschedulable := current.Annotations[GetUpgradeInitialStateAnnotationKey()]
			unschedulable = current.Annotations[DriverManagerCordonClaimAnnotation] == trueString ||
				dmInitialState || initiallyUnschedulable
		}

		updated, err := m.patchNodeSchedulingState(ctx, current, unschedulable, annotations)
		if err != nil {
			return err
		}
		updated.DeepCopyInto(node)
		return nil
	})
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
		ctx, node.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
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
