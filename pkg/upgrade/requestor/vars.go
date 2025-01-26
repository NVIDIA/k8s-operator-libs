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

import "os"

// TODO: This should be move to requestor's context (user)
func InitEnvs() {
	if os.Getenv("MAINTENANCE_OPERATOR_ENABLED") == "true" {
		//UseMaintenanceOperator = true
	}
	if os.Getenv("MAINTENANCE_OPERATOR_REQUESTOR_NAMESPACE") != "" {
		//MaintenanceOPRequestorNS = os.Getenv("MAINTENANCE_OPERATOR_REQUESTOR_NAMESPACE")
	}
	if os.Getenv("MAINTENANCE_OPERATOR_REQUESTOR_ID") != "" {
		//MaintenanceOPRequestorID = os.Getenv("MAINTENANCE_OPERATOR_REQUESTOR_ID")
	}
	if os.Getenv("MAINTENANCE_OPERATOR_POD_EVICTION_FILTERS") != "" {
		//*MaintenanceOPPodEvictionFilter = os.Getenv("MAINTENANCE_OPERATOR_POD_EVICTION_FILTERS")
	}
}
