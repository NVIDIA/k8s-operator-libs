package upgrade

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func daemonSetNodeAffinity(nodeName string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchFields: []corev1.NodeSelectorRequirement{{
					Key:      metav1.ObjectNameField,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{nodeName},
				}},
			}},
		},
	}}
}

func pendingReplacement(name string, owner types.UID, nodeName string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, OwnerReferences: []metav1.OwnerReference{controllerReference(owner)}},
		Spec:       corev1.PodSpec{Affinity: daemonSetNodeAffinity(nodeName)},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
}

func boundPod(name string, owner types.UID, nodeName string) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{NodeName: nodeName},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if owner != NoOwner {
		pod.OwnerReferences = []metav1.OwnerReference{controllerReference(owner)}
	}
	return pod
}

func TestPendingReplacementTarget(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		name     string
		mutate   func(*corev1.Pod)
		wantNode string
		wantOK   bool
	}{
		{name: "affinity written by the DaemonSet controller", mutate: func(*corev1.Pod) {}, wantNode: "node-a", wantOK: true},
		{name: "bound pod", mutate: func(p *corev1.Pod) { p.Spec.NodeName = "node-a" }},
		{name: "running pod", mutate: func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning }},
		{name: "terminating pod", mutate: func(p *corev1.Pod) { p.DeletionTimestamp = &now }},
		{name: "no affinity", mutate: func(p *corev1.Pod) { p.Spec.Affinity = nil }},
		{name: "no node affinity", mutate: func(p *corev1.Pod) { p.Spec.Affinity.NodeAffinity = nil }},
		{name: "no required node affinity", mutate: func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nil
		}},
		{name: "two terms", mutate: func(p *corev1.Pod) {
			req := p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			req.NodeSelectorTerms = append(req.NodeSelectorTerms, req.NodeSelectorTerms[0])
		}},
		{name: "term with match expressions", mutate: func(p *corev1.Pod) {
			term := &p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0]
			term.MatchExpressions = []corev1.NodeSelectorRequirement{{Key: "gpu", Operator: corev1.NodeSelectorOpExists}}
		}},
		{name: "two match fields", mutate: func(p *corev1.Pod) {
			term := &p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0]
			term.MatchFields = append(term.MatchFields, term.MatchFields[0])
		}},
		{name: "other field key", mutate: func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Key = "metadata.namespace"
		}},
		{name: "NotIn operator", mutate: func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Operator = corev1.NodeSelectorOpNotIn
		}},
		{name: "two target nodes", mutate: func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Values = []string{"node-a", "node-b"}
		}},
		{name: "empty target node", mutate: func(p *corev1.Pod) {
			p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Values = []string{""}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := pendingReplacement("pod", "ds", "node-a")
			tt.mutate(&pod)
			node, ok := pendingReplacementTarget(&pod)
			if node != tt.wantNode || ok != tt.wantOK {
				t.Errorf("pendingReplacementTarget() = (%q, %v), want (%q, %v)", node, ok, tt.wantNode, tt.wantOK)
			}
		})
	}
}

func TestReleaseOrphanHeldReservations(t *testing.T) {
	dsA := types.UID("ds-a")
	dsB := types.UID("ds-b")
	tests := []struct {
		name        string
		unaccounted map[types.UID]int
		pods        []corev1.Pod
		want        map[types.UID]int
	}{
		{
			name:        "orphan on A releases A, not B with more missing nodes",
			unaccounted: map[types.UID]int{dsA: 1, dsB: 2},
			pods: []corev1.Pod{
				boundPod("orphan", NoOwner, "node-a"),
				pendingReplacement("a-new", dsA, "node-a"),
			},
			want: map[types.UID]int{dsB: 2},
		},
		{
			name:        "orphan on A releases A when A and B miss the same number",
			unaccounted: map[types.UID]int{dsA: 1, dsB: 1},
			pods: []corev1.Pod{
				boundPod("orphan", NoOwner, "node-a"),
				pendingReplacement("a-new", dsA, "node-a"),
			},
			want: map[types.UID]int{dsB: 1},
		},
		{
			name:        "no pending replacement yet keeps the reservation",
			unaccounted: map[types.UID]int{dsA: 1, dsB: 1},
			pods:        []corev1.Pod{boundPod("orphan", NoOwner, "node-a")},
			want:        map[types.UID]int{dsA: 1, dsB: 1},
		},
		{
			name:        "replacement pinned to another node releases nothing",
			unaccounted: map[types.UID]int{dsA: 1},
			pods: []corev1.Pod{
				boundPod("orphan", NoOwner, "node-a"),
				pendingReplacement("a-new", dsA, "node-z"),
			},
			want: map[types.UID]int{dsA: 1},
		},
		{
			name:        "duplicate orphans and duplicate replacements release once",
			unaccounted: map[types.UID]int{dsA: 2},
			pods: []corev1.Pod{
				boundPod("orphan-1", NoOwner, "node-a"),
				boundPod("orphan-2", NoOwner, "node-a"),
				pendingReplacement("a-new-1", dsA, "node-a"),
				pendingReplacement("a-new-2", dsA, "node-a"),
			},
			want: map[types.UID]int{dsA: 1},
		},
		{
			name:        "two DaemonSets targeting the orphan's node release nothing",
			unaccounted: map[types.UID]int{dsA: 1, dsB: 1},
			pods: []corev1.Pod{
				boundPod("orphan", NoOwner, "node-a"),
				pendingReplacement("a-new", dsA, "node-a"),
				pendingReplacement("b-new", dsB, "node-a"),
			},
			want: map[types.UID]int{dsA: 1, dsB: 1},
		},
		{
			name:        "orphan node already holding a bound DaemonSet pod releases nothing",
			unaccounted: map[types.UID]int{dsA: 1},
			pods: []corev1.Pod{
				boundPod("orphan", NoOwner, "node-a"),
				boundPod("a-bound", dsA, "node-a"),
				pendingReplacement("a-new", dsA, "node-a"),
			},
			want: map[types.UID]int{dsA: 1},
		},
		{
			name:        "orphans on two nodes each release their own DaemonSet",
			unaccounted: map[types.UID]int{dsA: 1, dsB: 3},
			pods: []corev1.Pod{
				boundPod("orphan-a", NoOwner, "node-a"),
				boundPod("orphan-b", NoOwner, "node-b"),
				pendingReplacement("a-new", dsA, "node-a"),
				pendingReplacement("b-new", dsB, "node-b"),
			},
			want: map[types.UID]int{dsB: 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for run := 0; run < 50; run++ {
				state := NewClusterUpgradeState()
				for uid, n := range tt.unaccounted {
					state.UnaccountedNodes[uid] = n
				}
				podsByOwner := map[types.UID][]corev1.Pod{}
				for _, pod := range tt.pods {
					owner := NoOwner
					if ref := metav1.GetControllerOfNoCopy(&pod); ref != nil {
						owner = ref.UID
					}
					podsByOwner[owner] = append(podsByOwner[owner], pod)
				}
				releaseOrphanHeldReservations(&state, podsByOwner, tt.pods)
				if !reflect.DeepEqual(state.UnaccountedNodes, tt.want) {
					t.Fatalf("run %d: UnaccountedNodes = %v, want %v", run, state.UnaccountedNodes, tt.want)
				}
			}
		})
	}
}
