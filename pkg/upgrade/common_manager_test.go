package upgrade

import (
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestGetPodsByDSOwner(t *testing.T) {
	ownerA := types.UID("owner-a")
	ownerB := types.UID("owner-b")
	unrelatedOwner := types.UID("unrelated-owner")
	daemonSets := map[types.UID]*appsv1.DaemonSet{
		ownerA: {ObjectMeta: metav1.ObjectMeta{UID: ownerA}},
		ownerB: {ObjectMeta: metav1.ObjectMeta{UID: ownerB}},
	}

	tests := []struct {
		name             string
		pods             []corev1.Pod
		wantPodsByOwner  map[types.UID][]string
		wantOrphanedPods []string
	}{
		{
			name: "group pods by known DaemonSet controller",
			pods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pod-a-1",
						OwnerReferences: []metav1.OwnerReference{
							{UID: types.UID("non-controller")},
							controllerReference(ownerA),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pod-b",
						OwnerReferences: []metav1.OwnerReference{controllerReference(ownerB)},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pod-a-2",
						OwnerReferences: []metav1.OwnerReference{controllerReference(ownerA)},
					},
				},
			},
			wantPodsByOwner: map[types.UID][]string{
				ownerA: {"pod-a-1", "pod-a-2"},
				ownerB: {"pod-b"},
			},
			wantOrphanedPods: []string{},
		},
		{
			name: "return pods without a controller as orphaned",
			pods: []corev1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "orphaned-pod"}},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pod-with-non-controlling-owner",
						OwnerReferences: []metav1.OwnerReference{{UID: types.UID("non-controller")}},
					},
				},
			},
			wantPodsByOwner:  map[types.UID][]string{},
			wantOrphanedPods: []string{"orphaned-pod", "pod-with-non-controlling-owner"},
		},
		{
			name: "exclude pods controlled by an unrelated owner",
			pods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "pod-with-unrelated-controller",
						OwnerReferences: []metav1.OwnerReference{controllerReference(unrelatedOwner)},
					},
				},
			},
			wantPodsByOwner:  map[types.UID][]string{},
			wantOrphanedPods: []string{},
		},
	}

	manager := &CommonUpgradeManagerImpl{Log: logr.Discard()}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podsByOwner, orphanedPods := manager.GetPodsByDSOwner(daemonSets, tt.pods)

			if got := podNamesByOwner(podsByOwner); !reflect.DeepEqual(got, tt.wantPodsByOwner) {
				t.Errorf("GetPodsByDSOwner() pods = %v, want %v", got, tt.wantPodsByOwner)
			}
			if got := podNames(orphanedPods); !reflect.DeepEqual(got, tt.wantOrphanedPods) {
				t.Errorf("GetPodsByDSOwner() orphaned pods = %v, want %v", got, tt.wantOrphanedPods)
			}

			for owner, wantPods := range tt.wantPodsByOwner {
				if got := podNames(manager.GetPodsOwnedbyDs(daemonSets[owner], tt.pods)); !reflect.DeepEqual(got, wantPods) {
					t.Errorf("GetPodsOwnedbyDs() pods = %v, want %v", got, wantPods)
				}
			}
		})
	}
}

func podNamesByOwner(podsByOwner map[types.UID][]corev1.Pod) map[types.UID][]string {
	namesByOwner := make(map[types.UID][]string, len(podsByOwner))
	for owner, pods := range podsByOwner {
		namesByOwner[owner] = podNames(pods)
	}
	return namesByOwner
}

func podNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for i := range pods {
		names = append(names, pods[i].Name)
	}
	return names
}

func controllerReference(uid types.UID) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{UID: uid, Controller: &controller}
}
