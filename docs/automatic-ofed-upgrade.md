# Automatic Driver Upgrade
When a containerized driver is reloaded on the node, all Pods which use a resource(GPU, secondary NIC etc) enabled by the driver will lose access to those in their containers. All PODs which use those resources need to be removed from the node before the driver Pod is reloaded on that node.

It is possible to do a driver upgrade manually by using an `OnDelete` UpdateStrategy. But this procedure requires a lot of manual actions and can be error prone.

This document describes the automatic upgrade flow for the containerized driver.

### Upgrade NVIDIA drivers automatically
* Following spec defines the UpgradePolicy for the Driver in the CustomResource

```
apiVersion: nvidia.com/v1
kind: CustomResource
metadata:
  name: example-custom-resource
  namespace: nvidia-operator
spec:
  driver:
    upgradePolicy:
      # autoUpgrade is a global switch for automatic upgrade feature
      # if set to false all other options are ignored
      autoUpgrade: true
      # maxParallelUpgrades indicates how many nodes can be upgraded in parallel
      # 0 means no limit, all nodes will be upgraded in parallel
      maxParallelUpgrades: 0
      # describes configuration for node drain during automatic upgrade
      drain:
        # allow node draining during upgrade
        enable: true
        # allow force draining
        force: false
        # specify a label selector to filter pods on the node that need to be drained
        podSelector: ""
        # specify the length of time in seconds to wait before giving up drain, zero means infinite
        # if not specified, the default is 300 seconds
        timeoutSeconds: 300
        # specify if should continue even if there are pods using emptyDir
        deleteEmptyDir: false
```

* To track each node's upgrade status separately, run `kubectl describe node <node_name> | grep nvidia.com/<driver-name>-driver-upgrade-state`. See [Node upgrade states](#node-upgrade-states) section describing each state.

### Safe driver loading

On Node startup, the containerized driver takes time to compile and load.
During that time, workloads might get scheduled on that Node.
When the driver is eventually loaded, all existing PODs using resources managed by the driver will lose access to them.
Some such PODs might silently fail or hang.
To avoid such a situation, before the containerized driver is loaded,
the Node should get Cordoned and Drained to ensure all workloads are rescheduled.
The Node should be un-cordoned when the driver is ready on it.

The safe driver loading feature is implemented as a part of the upgrade flow, 
meaning safe driver loading is a special scenario of the upgrade procedure, 
where we upgrade from the inbox driver (driver which is installed on the host) to the containerized driver.

The default safe load implementation in the library assumes two-step driver loading procedure.
As a first step, the driver pod should load the [init container](https://github.com/Mellanox/network-operator-init-container),
which will set "safe driver load annotation" (`nvidia.com/<driver-name>-driver-upgrade.driver-wait-for-safe-load`)
on the node object, then the container blocks until the upgrade library removes the annotation from the node object.
When the init container completes successfully (when the annotation was removed from the Node object),
the driver Pod will proceed to the second step and do the driver loading.
After that, the upgrade library will wait for the driver to become ready and then Uncordon the node if required.

There is no need to enable the safe driver load feature in the upgrade library explicitly.
The feature will automatically kick in if "safe driver load annotation" is present on the Node object.

### Details
#### Node upgrade states
Each node's upgrade status is reflected in its `nvidia.com/<driver-name>-driver-upgrade-state` label. This label can have the following values:
*  Unknown (empty) node has this state when the upgrade flow is disabled or the node hasn't been processed yet
* `upgrade-required`  is set when the driver pod on the node is not up-to-date and required upgrade or if the driver is waiting for safe load
* `cordon-required` is set when the node needs to be made unschedulable in preparation for driver upgrade
* `wait-for-jobs-required` is set on the node when we need to wait on jobs to complete until given timeout
* `drain-required` is set when the node is required to be scheduled for drain
* `pod-restart-required` is set when the driver pod on the node is scheduled for restart 
or when unblock of the driver loading is required (safe driver load)
* `validation-required` is set when validation of the new driver deployed on the node is required before moving to `uncordon-required`
* `uncordon-required` is set when driver pod on the node is up-to-date and has "Ready" status
* `upgrade-done` is set when driver pod is up to date and running on the node, the node is schedulable
* `node-maintenance-required` is set for requestor mode upgrade (e.g.`MAINTENANCE_OPERATOR_ENABLED=true`) post `upgrade-required` state. Essentially it will create a matching nodeMaintenance object for maintenance operator to perform its node operations.
* `post-maintenance-required` is set when node maintenance status condition is `ready`, meaning maintenance operator has completed 
cordoning and draining related node(s), and now requestor (client) needs to perform post operations (e.g restart driver's pod, restart node, etc.)
* `upgrade-failed` is set when there are any failures during the driver upgrade, see [Troubleshooting](#node-is-in-drain-failed-state) section for more details.

#### State change diagram

_NOTE: the diagram is outdated_

![State change diagram](images/driver-upgrade-state-diagram.png)

#### Upgrade modes
##### in-place
in-place (legacy) mode is incorporating full driver upgrade lifecycle, including nodes operations e.g. cordon, pod eviction, drain, uncordon.
It also maintains an internal scheduler for performing above node operations, according
to provided `maxParallelUpgrades` under `UpgradePolicy`.

##### requestor
The new `requestor` upgrade mode uses the [NVIDIA maintenance operator](https://github.com/Mellanox/maintenance-operator) nodeMaintenance k8s API objects, to initiate the DOCA driver upgrade process.
Essentially, it will retire current upgrade controller (in-place mode) from performing the following node operations: cordon, wait for pods completion, drain, uncordon.
To enable requestor mode, the following environment variable should be enabled `MAINTENANCE_OPERATOR_ENABLED=true`.
requestor also exposes controller-runtime predicate functions under `pkg/upgrade/requestor/predicate.go`, embed in your
controller manager watchers:
```
  ctrl.NewControllerManagedBy(mgr).For(*).
  ...
  Watches(&maintenancev1alpha1.NodeMaintenance{}, createUpdateDeleteEnqueue,
			builder.WithPredicates(requestor.NewConditionChangedPredicate(setupLog,
		requestorOpts.MaintenanceOPRequestorID))).
```
* Make sure that NVIDIA maintenance-operator pod is running.

> __Note__: Initially `k8s-operator-libs` will support both `requestor`, `inplace` (legacy) modes simultaneously.
> Meaning in case node undergoes upgrade prior to enabling `requestor` mode, node will continue `inplace` upgrade mode. Only after `requestor` mode is set, and upgrade
> controller has set nodes state to be upgrade-required, only then new requestor mode will take place.

###### shared-requestor
The requestor mode supports a `shared-requestor` flow where multiple operators can coordinate node maintenance operations:
Assumptions:
1. Cluster admin, which requires `shared-requestor` flow, needs to make sure that all operators, utilizing maintenance OP, use same upgrade policy specs (same drainSpec).
2. To be able to accommodate both GPU/Network drivers upgrade, `DrainSpec.PodSelector` should be set accordingly (hard-coded). 
 * podSelector: `nvidia.com/ofed-driver-upgrade-drain.skip!=true,nvidia.com/gpu-driver-upgrade-drain.skip!=true`
3. No custom `NodeMaintenanceNamePrefix` should be used. Requestor will use `DefaultNodeMaintenanceNamePrefix` as a common prefix for nodeMaintenance name.
Flow:
1. When a nodeMaintenance object exists, additional operators append their requestorID to the spec.AdditionalRequestors list, using patch with optimistic lock
2. During `uncordon-required` completion:
   - Non-owning operators remove themselves from spec.AdditionalRequestors list using patch with optimistic lock
   - Each operator removes its dedicated label from the nodeMaintenance object
3. The owning nodeMaintenance operator handles the actual, client side, deletion of the nodeMaintenance object

> __Note__: `owning operator`
> Its the operator that managed to create the `NodeMaintenance` object.
> which means for a given `NodeMaintenance` obj (name of obj is the same in the shared-requestor mode for all cooperating operators) its the operator whose RequestorID is set under `spec.requestorID`.
> `non owning` operator
> Its the operator that did not create the `NodeMaintenances` object, which means for a given `NodeMaintenance` obj its the operator whose RequestorID is present under `spec.AdditionalRequestors`


### Troubleshooting
#### Node is in `upgrade-failed` state
* Drain the node manually by running `kubectl drain <node_name> --ignore-daemonsets`
* Delete the driver pod on the node manually by running the following command:

```
kubectl delete pod -n `kubectl get -A pods --field-selector spec.nodeName=<node_name> -l <driver-pod-label> --no-headers | awk '{print $1" "$2}'`
```

* Wait for the node to finish upgrading
#### Updated driver pod failed to start / New version of driver can't install on the node
* Manually delete the pod using by using `kubectl delete -n <operator-namespace> <pod_name>`
* If after the restart the pod still fails, change the driver version in the CustomResource to the previous or other working version