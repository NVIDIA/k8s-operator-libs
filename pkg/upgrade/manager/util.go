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

import "github.com/NVIDIA/k8s-operator-libs/pkg/upgrade/base"

// SetDriverName is a wrapper to be used by external callers importing upgrade pkg
func SetDriverName(driver string) {
	base.SetDriverName(driver)
}

// GetUpgradeStateLabelKey is a wrapper to be used by external callers importing upgrade pkg
func GetUpgradeStateLabelKey() string {
	return base.GetUpgradeStateLabelKey()
}

// GetUpgradeSkipNodeLabelKey is a wrapper to be used by external callers importing upgrade pkg
func GetUpgradeSkipNodeLabelKey() string {
	return base.GetUpgradeSkipNodeLabelKey()
}

// GetUpgradeRequestedAnnotationKey is a wrapper to be used by external callers importing upgrade pkg
func GetUpgradeRequestedAnnotationKey() string {
	return base.GetUpgradeRequestedAnnotationKey()
}
