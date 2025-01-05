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
	"context"
	"fmt"
	"reflect"

	//nolint:depguard
	maintenancev1alpha1 "github.com/Mellanox/maintenance-operator/api/v1alpha1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
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
	Name string
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
	reqLog.V(consts.LogLevelInfo).Info("reconcile NodeMaintenance request")
	nm := &maintenancev1alpha1.NodeMaintenance{}
	if err := r.Get(ctx, req.NamespacedName, nm); err != nil {
		return reconcile.Result{}, err
	}

	if !nm.GetDeletionTimestamp().IsZero() {
		// object is being deleted
		// TODO: wait for object deletion to update node uncodron is finished
		return reconcile.Result{}, nil
	}

	cond := meta.FindStatusCondition(nm.Status.Conditions, maintenancev1alpha1.ConditionReasonReady)
	if cond != nil {
		reqLog.V(consts.LogLevelInfo).Info("node maintenance operation completed", nm.Spec.NodeName, cond.Reason)
		if cond.Reason == maintenancev1alpha1.ConditionReasonReady ||
			cond.Reason == maintenancev1alpha1.ConditionReasonFailedMaintenance {
			//TODO: Add a channel to push node name + ready condition state
			// Upgrade manager should wait for channel and update to UpgradeStatePodRestartRequired
			r.StatusCh <- NodeMaintenanceCondition{Name: nm.Spec.NodeName, Reason: cond.Reason}
		}
	}

	return reconcile.Result{}, nil
}

func (r *NodeMaintenanceReconciler) SetupWithManager(mgr ctrl.Manager, log logr.Logger) error {
	log.V(consts.LogLevelInfo).Info("Started nodeMaintenance status manger")
	statusPredicate := predicate.Funcs{
		// Don't reconcile on create
		CreateFunc: func(e event.CreateEvent) bool {
			_ = e
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// TODO: do we need to watch for deletion?
			log.V(consts.LogLevelInfo).Info("Delete event detected, triggering reconcile",
				e.Object.GetName(), e.Object.GetNamespace())
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, okOld := e.ObjectOld.DeepCopyObject().(*maintenancev1alpha1.NodeMaintenance)
			newObj, okNew := e.ObjectNew.DeepCopyObject().(*maintenancev1alpha1.NodeMaintenance)

			if !okOld || !okNew {
				log.V(consts.LogLevelError).Error(fmt.Errorf("type assertion failed"), "unable to cast object to NodeMaintenance")
				return false
			}
			// Reconcile only when the Status field changes
			return !reflect.DeepEqual(oldObj.Status, newObj.Status) && oldObj.Generation == newObj.Generation
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&maintenancev1alpha1.NodeMaintenance{}, builder.WithPredicates(statusPredicate)).
		Named(MaintenanceOPControllerName).
		Complete(r)
}
