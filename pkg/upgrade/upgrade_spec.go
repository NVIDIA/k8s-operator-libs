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
	"fmt"
	"strings"
)

var (
	// DriverName is the name of the driver to be managed by this package
	DriverName string
)

// DriverUpgradePolicySpec describes policy configuration for automatic upgrades
type DriverUpgradePolicySpec struct {
	// AutoUpgrade is a global switch for automatic upgrade feature
	// if set to false all other options are ignored
	// +optional
	// +kubebuilder:default:=false
	AutoUpgrade bool `json:"autoUpgrade,omitempty"`
	// MaxParallelUpgrades indicates how many nodes can be upgraded in parallel
	// 0 means no limit, all nodes will be upgraded in parallel
	// +optional
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum:=0
	MaxParallelUpgrades int                    `json:"maxParallelUpgrades,omitempty"`
	PodDeletion         *PodDeletionSpec       `json:"podDeletion,omitempty"`
	WaitForCompletion   *WaitForCompletionSpec `json:"waitForCompletion,omitempty"`
	DrainSpec           *DrainSpec             `json:"drain,omitempty"`
}

// WaitForCompletionSpec describes the configuration for waiting on job completions
type WaitForCompletionSpec struct {
	// PodSelector specifies a label selector for the pods to wait for completion
	// For more details on label selectors, see:
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	PodSelector string `json:"podSelector,omitempty"`
	// TimeoutSecond specifies the length of time in seconds to wait before giving up on pod termination, zero means infinite
	// +optional
	// +kubebuilder:default:=0
	// +kubebuilder:validation:Minimum:=0
	TimeoutSecond int `json:"timeoutSeconds,omitempty"`
}

// PodDeletionSpec describes configuration for deletion of pods using special resources during automatic upgrade
type PodDeletionSpec struct {
	// Force indicates if force deletion is allowed
	// +optional
	// +kubebuilder:default:=false
	Force bool `json:"force,omitempty"`
	// TimeoutSecond specifies the length of time in seconds to wait before giving up on pod termination, zero means infinite
	// +optional
	// +kubebuilder:default:=300
	// +kubebuilder:validation:Minimum:=0
	TimeoutSecond int `json:"timeoutSeconds,omitempty"`
	// DeleteEmptyDir indicates if should continue even if there are pods using emptyDir
	// (local data that will be deleted when the pod is deleted)
	// +optional
	// +kubebuilder:default:=false
	DeleteEmptyDir bool `json:"deleteEmptyDir,omitempty"`
}

// SetDriverName sets the name of the driver managed by the upgrade package
func SetDriverName(driver string) {
	DriverName = driver
}

// GetUpgradeStateLabelKey returns state label key used for upgrades
func GetUpgradeStateLabelKey() string {
	return fmt.Sprintf(UpgradeStateLabelKeyFmt, DriverName)
}

// GetUpgradeSkipNodeLabelKey returns node label used to skip upgrades
func GetUpgradeSkipNodeLabelKey() string {
	return fmt.Sprintf(UpgradeSkipNodeLabelKeyFmt, DriverName)
}

// GetUpgradeSkipDrainPodLabelKey returns pod label used to skip eviction during drain
func GetUpgradeSkipDrainPodLabelKey() string {
	return fmt.Sprintf(UpgradeSkipDrainPodLabelKeyFmt, DriverName)
}

// GetEventReason returns the reason type based on the driver name
func GetEventReason() string {
	return fmt.Sprintf("%sDriverUpgrade", strings.ToUpper(DriverName))
}
