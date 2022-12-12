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

* To track each node's upgrade status separately, run `kubectl describe node <node_name> | grep nvidia.com/<driver-name>-upgrade-state`. See [Node upgrade states](#node-upgrade-states) section describing each state.

### Details
#### Node upgrade states
Each node's upgrade status is reflected in its `nvidia.com/<driver-name>-upgrade-state` label. This label can have the following values:
* Unknown (empty): node has this state when the upgrade flow is disabled or the node hasn't been processed yet
* `upgrade-done` is set when the driver Pod is up to date and running on the node, the node is schedulable
UpgradeStateDone = "upgrade-done"
* `upgrade-required` is set when the driver Pod on the node is not up-to-date and requires upgrade. No actions are performed at this stage
* `cordon` is set when the node is set to cordon. After this no new workloads can be scheduled onto the node until the upgrade is complete.
* `wait-for-completion` is set when the node is set to wait for running workloads to complete. This state can be optional.
* `drain` is set when the node is scheduled for drain. After the drain the state is changed either to `pod-restart` or `drain-failed`
UpgradeStateDrain = "drain"
* `pod-restart` is set when the driver Pod on the node is scheduler for restart. After the restart state is changed to `uncordon-required`
* `drain-failed` is set when drain on the node has failed. Manual interaction is required at this stage. See [Troubleshooting](#node-is-in-drain-failed-state) section for more details.
* `uncordon-required` is set when the driver Pod on the node is up-to-date and has "Ready" status. After uncordone the state is changed to `upgrade-done`

#### State change diagram

![State change diagram](images/driver-upgrade-state-diagram.png)

### Troubleshooting
#### Node is in `drain-failed` state
* Drain the node manually by running `kubectl drain <node_name> --ignore-daemonsets`
* Delete the driver pod on the node manually by running the following command:

```
kubectl delete pod -n `kubectl get -A pods --field-selector spec.nodeName=<node_name> -l <driver-pod-label> --no-headers | awk '{print $1" "$2}'`
```

* Wait for the node to finish upgrading
#### Updated driver pod failed to start / New version of driver can't install on the node
* Manually delete the pod using by using `kubectl delete -n <operator-namespace> <pod_name>`
* If after the restart the pod still fails, change the driver version in the CustomResource to the previous or other working version