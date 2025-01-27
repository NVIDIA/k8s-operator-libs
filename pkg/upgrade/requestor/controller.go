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

import (
	"cmp"
	"context"
	"reflect"
	"slices"

	//nolint:depguard
	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/NVIDIA/k8s-operator-libs/pkg/consts"
)

var Scheme = runtime.NewScheme()

// NodeMaintenanceConditionReady designates maintenance operation completed for a designated node
type NodeMaintenanceCondition struct {
	// Node maintenance name
	Name     string
	NodeName string
	// Node maintenance condition reason
	Reason string
}

// NodeMaintenanceReconciler reconciles a NodeMaintenance object
type NodeMaintenanceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	StatusCh chan NodeMaintenanceCondition
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(Scheme))
	utilruntime.Must(maintenancev1alpha1.AddToScheme(Scheme))
	//+kubebuilder:scaffold:scheme
}

//nolint:dupl
func (r *NodeMaintenanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLog := log.FromContext(ctx)
	reqLog.V(consts.LogLevelDebug).Info("reconcile NodeMaintenance request", req.Name, req.Namespace)

	// get NodeMaintenance object
	nm := &maintenancev1alpha1.NodeMaintenance{}
	if err := r.Get(ctx, req.NamespacedName, nm); err != nil {
		if k8serrors.IsNotFound(err) {
			reqLog.Info("NodeMaintenance object not found, nothing to do.")
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	cond := meta.FindStatusCondition(nm.Status.Conditions, maintenancev1alpha1.ConditionReasonReady)
	if cond != nil {
		if len(nm.Finalizers) == 0 || !nm.GetDeletionTimestamp().IsZero() {
			// object is being deleted
			// TODO: wait for object deletion to update node uncodron is finished
			reqLog.V(consts.LogLevelDebug).Info("node maintenance is beening deleted", nm.Name, nm.Spec.NodeName)
			r.StatusCh <- NodeMaintenanceCondition{Name: nm.Name, NodeName: nm.Spec.NodeName, Reason: "deleting"}
			return reconcile.Result{}, nil
		}
		reqLog.V(consts.LogLevelDebug).Info("node maintenance operation completed", nm.Spec.NodeName, cond.Reason)
		if cond.Reason == maintenancev1alpha1.ConditionReasonReady ||
			cond.Reason == maintenancev1alpha1.ConditionReasonFailedMaintenance {
			//TODO: Add a channel to push node name + ready condition state
			// Upgrade manager should wait for channel and update to UpgradeStatePodRestartRequired
			r.StatusCh <- NodeMaintenanceCondition{Name: nm.Name, NodeName: nm.Spec.NodeName, Reason: cond.Reason}
			return reconcile.Result{}, nil
		}
	}

	if !nm.GetDeletionTimestamp().IsZero() {
		// object is being deleted
		// TODO: wait for object deletion to update node uncodron is finished
		reqLog.V(consts.LogLevelDebug).Info("node maintenance is beening deleted", nm.Name, nm.Spec.NodeName)
		r.StatusCh <- NodeMaintenanceCondition{Name: nm.Name, NodeName: nm.Spec.NodeName, Reason: "deleting"}
		return reconcile.Result{}, nil
	}

	reqLog.V(consts.LogLevelWarning).Info("nothing todo, check why", nm.Name, nm.Spec.NodeName)

	return reconcile.Result{}, nil
}

func (r *NodeMaintenanceReconciler) SetupWithManager(mgr ctrl.Manager, log logr.Logger, requestorID string) error {
	log.V(consts.LogLevelInfo).Info("Started nodeMaintenance status manger")

	return ctrl.NewControllerManagedBy(mgr).
		For(&maintenancev1alpha1.NodeMaintenance{}, builder.WithPredicates(NewConditionChangedPredicate(log, requestorID))).
		Named(MaintenanceOPControllerName).
		Complete(r)
}

// NewConditionChangedPredicate creates a new ConditionChangedPredicate
func NewConditionChangedPredicate(log logr.Logger, requestorID string) ConditionChangedPredicate {
	return ConditionChangedPredicate{
		Funcs:       predicate.Funcs{},
		log:         log,
		requestorID: requestorID,
	}
}

type ConditionChangedPredicate struct {
	predicate.Funcs
	requestorID string

	log logr.Logger
}

// Update implements Predicate.
func (p ConditionChangedPredicate) Update(e event.TypedUpdateEvent[client.Object]) bool {
	p.log.V(consts.LogLevelDebug).Info("ConditionChangedPredicate Update event")

	// TODO: Add predicate for specific (known) Requestor ID

	if e.ObjectOld == nil {
		p.log.Error(nil, "old object is nil in update event, ignoring event.")
		return false
	}
	if e.ObjectNew == nil {
		p.log.Error(nil, "new object is nil in update event, ignoring event.")
		return false
	}

	oldO, ok := e.ObjectOld.(*maintenancev1alpha1.NodeMaintenance)
	if !ok {
		p.log.Error(nil, "failed to cast old object to NodeMaintenance in update event, ignoring event.")
		return false
	}

	newO, ok := e.ObjectNew.(*maintenancev1alpha1.NodeMaintenance)
	if !ok {
		p.log.Error(nil, "failed to cast new object to NodeMaintenance in update event, ignoring event.")
		return false
	}

	// check for matching requestor ID
	if newO.Spec.RequestorID != p.requestorID {
		return false
	}

	cmpByType := func(a, b metav1.Condition) int {
		return cmp.Compare(a.Type, b.Type)
	}

	// sort old and new obj.Status.Conditions so they can be compared using DeepEqual
	slices.SortFunc(oldO.Status.Conditions, cmpByType)
	slices.SortFunc(newO.Status.Conditions, cmpByType)

	condChanged := !reflect.DeepEqual(oldO.Status.Conditions, newO.Status.Conditions)
	// Check if the object is marked for deletion
	deleting := len(newO.Finalizers) == 0 && len(oldO.Finalizers) > 0
	deleting = deleting || !newO.DeletionTimestamp.IsZero()
	enqueue := condChanged || deleting

	p.log.V(consts.LogLevelDebug).Info("update event for NodeMaintenance",
		"name", newO.Name, "namespace", newO.Namespace,
		"condition-changed", condChanged,
		"deleting", deleting, "enqueue-request", enqueue)

	return enqueue
}
