/*
Copyright 2026 NVIDIA CORPORATION & AFFILIATES

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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/NVIDIA/k8s-operator-libs/api/upgrade/v1alpha1"
)

const syncTestTimeout = 30 * time.Second

// hashOnlyPodManager wraps a real PodManagerImpl but answers revision hashes without a
// cluster, so BuildState can run. Pod and DaemonSet hashes match, which keeps the node out of
// upgrade-required.
type hashOnlyPodManager struct{ *PodManagerImpl }

func (hashOnlyPodManager) GetPodControllerRevisionHash(*corev1.Pod) (string, error) {
	return "rev", nil
}

func (hashOnlyPodManager) GetDaemonsetControllerRevisionHash(context.Context, *appsv1.DaemonSet) (string, error) {
	return "rev", nil
}

// heldPatch is one node state PATCH the fake API server holds until released.
type heldPatch struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *heldPatch) releaseOnce() { h.once.Do(func() { close(h.release) }) }

// asyncRaceHarness runs a real asynchronous worker (drain manager or pod eviction) and a real
// NodeUpgradeStateProvider against a fake API server that holds the first two state PATCHes
// on the node until released. Holding the first PATCH parks the worker after it took the
// node lock and before its state change lands: the window where a snapshot can capture the
// old state for a node that is about to leave it. The second PATCH is the durable sign that
// a second worker was scheduled for the same node.
type asyncRaceHarness struct {
	ctx          context.Context
	client       client.WithWatch
	manager      *ClusterUpgradeStateManagerImpl
	drainManager *DrainManagerImpl
	podManager   *PodManagerImpl
	node         *corev1.Node
	pod          *corev1.Pod
	ds           *appsv1.DaemonSet
	drainSpec    *v1alpha1.DrainSpec
	deletionSpec *v1alpha1.PodDeletionSpec
	patches      [2]*heldPatch
	nodesListed  chan struct{}
}

func newAsyncRaceHarness(t *testing.T, initialState string) *asyncRaceHarness {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	SetDriverName("gpu")
	h := &asyncRaceHarness{
		ctx:          context.Background(),
		nodesListed:  make(chan struct{}, 8),
		drainSpec:    &v1alpha1.DrainSpec{Enable: true, Force: true, TimeoutSecond: 30},
		deletionSpec: &v1alpha1.PodDeletionSpec{Force: true, TimeoutSecond: 30},
	}
	for i := range h.patches {
		h.patches[i] = &heldPatch{started: make(chan struct{}, 1), release: make(chan struct{})}
	}
	t.Cleanup(func() {
		for _, p := range h.patches {
			p.releaseOnce()
		}
	})
	h.node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1",
		Labels: map[string]string{GetUpgradeStateLabelKey(): initialState}}}
	h.ds = &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "driver", Namespace: "ns", UID: "ds-uid",
		Labels: map[string]string{"app": "driver"}},
		Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: 1}}
	controller := true
	h.pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "ns",
		Labels: map[string]string{"app": "driver", PodControllerRevisionHashLabelKey: "rev"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet",
			Name: "driver", UID: "ds-uid", Controller: &controller}}},
		Spec:   corev1.PodSpec{NodeName: "n1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}

	var patchCount int
	var mu sync.Mutex
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(h.node, h.ds, h.pod).Build()
	h.client = interceptor.NewClient(base, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch,
			opts ...client.PatchOption) error {
			if n, ok := obj.(*corev1.Node); ok && n.Name == "n1" {
				mu.Lock()
				idx := patchCount
				patchCount++
				mu.Unlock()
				if idx < len(h.patches) {
					h.patches[idx].started <- struct{}{}
					<-h.patches[idx].release
				}
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			err := c.List(ctx, list, opts...)
			if _, ok := list.(*corev1.NodeList); ok {
				h.nodesListed <- struct{}{}
			}
			return err
		},
	})
	clientset := k8sfake.NewSimpleClientset(h.node.DeepCopy())
	recorder := record.NewFakeRecorder(100)
	provider := NewNodeUpgradeStateProvider(h.client, logr.Discard(), recorder)
	h.drainManager = NewDrainManager(clientset, provider, logr.Discard(), recorder)
	h.podManager = NewPodManager(clientset, provider, logr.Discard(),
		func(corev1.Pod) bool { return false }, recorder)
	h.manager = &ClusterUpgradeStateManagerImpl{CommonUpgradeManagerImpl: &CommonUpgradeManagerImpl{
		Log: logr.Discard(), K8sClient: h.client, K8sInterface: clientset, EventRecorder: recorder,
		DrainManager: h.drainManager, PodManager: hashOnlyPodManager{h.podManager},
		NodeUpgradeStateProvider: provider, podDeletionStateEnabled: true,
	}}
	return h
}

func (h *asyncRaceHarness) await(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(syncTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitGuardClear blocks until the worker has left its scheduling guard. The guard is cleared
// by a defer after the state patch returns, so this is the point at which a stale snapshot
// could schedule a second worker. StringSet is synchronized and this observes real completion.
func (h *asyncRaceHarness) waitGuardClear(t *testing.T, guard *StringSet) {
	t.Helper()
	deadline := time.Now().Add(syncTestTimeout)
	for guard.Has("n1") {
		if time.Now().After(deadline) {
			t.Fatal("worker did not finish")
		}
		runtime.Gosched()
	}
}

// asyncWorkerCase describes one asynchronous state and how to drive its worker.
type asyncWorkerCase struct {
	name         string
	initialState string
	guard        func(h *asyncRaceHarness) *StringSet
	start        func(h *asyncRaceHarness, nodes []*corev1.Node) error
	process      func(h *asyncRaceHarness, state *ClusterUpgradeState) error
}

func asyncWorkerCases() []asyncWorkerCase {
	return []asyncWorkerCase{
		{
			name:         "drain",
			initialState: UpgradeStateDrainRequired,
			guard:        func(h *asyncRaceHarness) *StringSet { return h.drainManager.drainingNodes },
			start: func(h *asyncRaceHarness, nodes []*corev1.Node) error {
				return h.drainManager.ScheduleNodesDrain(h.ctx, &DrainConfiguration{Spec: h.drainSpec, Nodes: nodes})
			},
			process: func(h *asyncRaceHarness, s *ClusterUpgradeState) error {
				return h.manager.ProcessDrainNodes(h.ctx, s, h.drainSpec)
			},
		},
		{
			name:         "pod eviction",
			initialState: UpgradeStatePodDeletionRequired,
			guard:        func(h *asyncRaceHarness) *StringSet { return h.podManager.nodesInProgress },
			start: func(h *asyncRaceHarness, nodes []*corev1.Node) error {
				return h.podManager.SchedulePodEviction(h.ctx,
					&PodManagerConfig{Nodes: nodes, DeletionSpec: h.deletionSpec, DrainEnabled: true})
			},
			process: func(h *asyncRaceHarness, s *ClusterUpgradeState) error {
				return h.manager.ProcessPodDeletionRequiredNodes(h.ctx, s, h.deletionSpec, true)
			},
		},
	}
}

// TestBuildStateWaitsBehindAsyncWorker: a snapshot built while a worker is mid-way through
// changing a node's state must wait behind the node lock and see the new state, so the
// following ApplyState does not schedule the same work again.
func TestBuildStateWaitsBehindAsyncWorker(t *testing.T) {
	for _, tc := range asyncWorkerCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newAsyncRaceHarness(t, tc.initialState)
			if err := tc.start(h, []*corev1.Node{h.node}); err != nil {
				t.Fatal(err)
			}
			h.await(t, h.patches[0].started, "the worker's state patch")

			type result struct {
				state *ClusterUpgradeState
				err   error
			}
			done := make(chan result, 1)
			go func() {
				state, err := h.manager.BuildState(h.ctx, "ns", map[string]string{"app": "driver"})
				done <- result{state, err}
			}()
			// The node LIST happens while the worker still holds the lock, so the listed node
			// carries the old state. The snapshot must not be built from it for this state.
			h.await(t, h.nodesListed, "BuildState's node list")
			h.patches[0].releaseOnce()

			var r result
			select {
			case r = <-done:
			case <-time.After(syncTestTimeout):
				t.Fatal("timed out waiting for BuildState")
			}
			if r.err != nil {
				t.Fatal(r.err)
			}
			// The central check: the snapshot reflects the worker's completed transition.
			if n := len(r.state.NodeStates[tc.initialState]); n != 0 {
				t.Fatalf("snapshot still shows %d node(s) in %s after the worker moved on", n, tc.initialState)
			}
			if n := len(r.state.NodeStates[UpgradeStatePodRestartRequired]); n != 1 {
				t.Fatalf("expected the node in pod-restart-required, got %v", stateNames(r.state))
			}

			h.waitGuardClear(t, tc.guard(h))
			if err := tc.process(h, r.state); err != nil {
				t.Fatal(err)
			}
			select {
			case <-h.patches[1].started:
				t.Fatal("a second worker was scheduled for a node that already finished")
			default:
			}
			if tc.guard(h).Has("n1") {
				t.Fatal("the node re-entered the scheduling guard")
			}
		})
	}
}

// TestStaleListSnapshotSchedulesSecondWorker is the comparison case: a snapshot built from
// the LIST alone, without the provider's lock, keeps the old state and, once the worker has
// cleared its guard, schedules the same work again. The second state PATCH is the durable
// evidence; the guard itself may already be clear again by the time we look.
func TestStaleListSnapshotSchedulesSecondWorker(t *testing.T) {
	for _, tc := range asyncWorkerCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newAsyncRaceHarness(t, tc.initialState)
			if err := tc.start(h, []*corev1.Node{h.node}); err != nil {
				t.Fatal(err)
			}
			h.await(t, h.patches[0].started, "the worker's state patch")

			listed := &corev1.NodeList{}
			if err := h.client.List(h.ctx, listed); err != nil {
				t.Fatal(err)
			}
			stale := NewClusterUpgradeState()
			stale.NodeStates[tc.initialState] = []*NodeUpgradeState{{
				Node: &listed.Items[0], DriverPod: h.pod, DriverDaemonSet: h.ds}}

			h.patches[0].releaseOnce()
			h.waitGuardClear(t, tc.guard(h))
			if err := tc.process(h, &stale); err != nil {
				t.Fatal(err)
			}
			h.await(t, h.patches[1].started, "a second worker's state patch, which the stale snapshot should cause")
			h.patches[1].releaseOnce()
			h.waitGuardClear(t, tc.guard(h))
		})
	}
}

func stateNames(s *ClusterUpgradeState) map[string]int {
	out := map[string]int{}
	for k, v := range s.NodeStates {
		out[k] = len(v)
	}
	return out
}
