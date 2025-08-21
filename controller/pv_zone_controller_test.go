package controller

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/diff"
	kubeinformers "k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"
)

type fixture struct {
	t *testing.T

	kubeClient *k8sfake.Clientset

	// Objects to put in the store.
	podLister  []*corev1.Pod
	pvLister   []*corev1.PersistentVolume
	pvcLister  []*corev1.PersistentVolumeClaim
	nodeLister []*corev1.Node

	// Actions expected to happen on the client.
	kubeActions []core.Action

	// Objects from here are pre-loaded into NewSimpleFake.
	kubeObjects []runtime.Object
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{}
	f.t = t
	f.kubeObjects = []runtime.Object{}
	return f
}

func (f *fixture) newController() (*PVZoneController, kubeinformers.SharedInformerFactory) {
	f.kubeClient = k8sfake.NewSimpleClientset(f.kubeObjects...)

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(f.kubeClient, 0)

	controller := &PVZoneController{
		kubeClient:       f.kubeClient,
		podLister:        kubeInformerFactory.Core().V1().Pods().Lister(),
		podListerSynced:  kubeInformerFactory.Core().V1().Pods().Informer().HasSynced,
		pvLister:         kubeInformerFactory.Core().V1().PersistentVolumes().Lister(),
		pvListerSynced:   kubeInformerFactory.Core().V1().PersistentVolumes().Informer().HasSynced,
		pvcLister:        kubeInformerFactory.Core().V1().PersistentVolumeClaims().Lister(),
		pvcListerSynced:  kubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().HasSynced,
		nodeLister:       kubeInformerFactory.Core().V1().Nodes().Lister(),
		nodeListerSynced: kubeInformerFactory.Core().V1().Nodes().Informer().HasSynced,
		workqueue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "PVZoneLabeler"),
	}

	for _, pod := range f.podLister {
		kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod)
	}

	for _, pv := range f.pvLister {
		kubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer().Add(pv)
	}

	for _, pvc := range f.pvcLister {
		kubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer().Add(pvc)
	}

	for _, node := range f.nodeLister {
		kubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer().Add(node)
	}

	return controller, kubeInformerFactory
}

func (f *fixture) run(podName string) {
	f.runController(podName, true, false)
}

func (f *fixture) runController(podName string, startInformers bool, expectError bool) {
	controller, kubeInformerFactory := f.newController()
	if startInformers {
		stopCh := make(chan struct{})
		defer close(stopCh)
		kubeInformerFactory.Start(stopCh)
	}

	err := controller.syncHandler(podName)
	if !expectError && err != nil {
		f.t.Errorf("error syncing pod: %v", err)
	} else if expectError && err == nil {
		f.t.Error("expected error syncing pod, got nil")
	}

	actions := filterInformerActions(f.kubeClient.Actions())
	for i, action := range actions {
		if len(f.kubeActions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(actions)-len(f.kubeActions), actions[i:])
			break
		}

		expectedAction := f.kubeActions[i]
		checkAction(expectedAction, action, f.t)
	}

	if len(f.kubeActions) > len(actions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.kubeActions)-len(actions), f.kubeActions[len(actions):])
	}
}

// checkAction verifies that expected and actual actions are equal and both have
// same attached resources
func checkAction(expected, actual core.Action, t *testing.T) {
	if !(expected.Matches(actual.GetVerb(), actual.GetResource().Resource) && expected.GetSubresource() == actual.GetSubresource()) {
		t.Errorf("Expected\n\t%#v\ngot\n\t%#v", expected, actual)
		return
	}

	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Errorf("Action has wrong type. Expected: %t. Got: %t", expected, actual)
		return
	}

	// Check object equality based on action type
	if updateAction, ok := actual.(core.UpdateAction); ok {
		expectedUpdateAction, _ := expected.(core.UpdateAction)
		expObject := expectedUpdateAction.GetObject()
		object := updateAction.GetObject()
		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				updateAction.GetVerb(), updateAction.GetResource().Resource, diff.ObjectGoPrintDiff(expObject, object))
		}
	} else if createAction, ok := actual.(core.CreateAction); ok {
		expectedCreateAction, _ := expected.(core.CreateAction)
		expObject := expectedCreateAction.GetObject()
		object := createAction.GetObject()
		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				createAction.GetVerb(), createAction.GetResource().Resource, diff.ObjectGoPrintDiff(expObject, object))
		}
	} else if patchAction, ok := actual.(core.PatchAction); ok {
		expectedPatchAction, _ := expected.(core.PatchAction)
		expPatch := expectedPatchAction.GetPatch()
		patch := patchAction.GetPatch()
		if !reflect.DeepEqual(expPatch, patch) {
			t.Errorf("Action %s %s has wrong patch\nDiff:\n %s",
				patchAction.GetVerb(), patchAction.GetResource().Resource, diff.ObjectGoPrintDiff(expPatch, patch))
		}
	}
}

// filterInformerActions filters list and watch actions for testing resources.
// Since list and watch don't change resource state we can filter it to lower
// noise level in our tests.
func filterInformerActions(actions []core.Action) []core.Action {
	ret := []core.Action{}
	for _, action := range actions {
		if len(action.GetNamespace()) == 0 &&
			(action.Matches("list", "pods") ||
				action.Matches("watch", "pods") ||
				action.Matches("list", "persistentvolumeclaims") ||
				action.Matches("watch", "persistentvolumeclaims") ||
				action.Matches("list", "persistentvolumes") ||
				action.Matches("watch", "persistentvolumes") ||
				action.Matches("list", "nodes") ||
				action.Matches("watch", "nodes")) {
			continue
		}
		ret = append(ret, action)
	}
	return ret
}

func TestCreatesZoneLabelsOnPV(t *testing.T) {
	f := newFixture(t)

	// Create test resources
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-a",
			},
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.test.com",
					VolumeHandle: "vol-123",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "default",
				Name:      "test-pvc",
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	f.nodeLister = append(f.nodeLister, node)
	f.pvLister = append(f.pvLister, pv)
	f.pvcLister = append(f.pvcLister, pvc)
	f.podLister = append(f.podLister, pod)

	f.kubeObjects = append(f.kubeObjects, node, pv, pvc, pod)

	// Expected update to PV with zone label
	updatedPV := pv.DeepCopy()
	if updatedPV.Labels == nil {
		updatedPV.Labels = make(map[string]string)
	}
	updatedPV.Labels[TopologyZoneLabel] = "us-west1-a"

	f.kubeActions = append(f.kubeActions, core.NewUpdateAction(
		schema.GroupVersionResource{Resource: "persistentvolumes"},
		"",
		updatedPV))

	// Run the test
	f.run("default/test-pod")
}

func TestDoesNotLabelPVWithoutReadyPods(t *testing.T) {
	f := newFixture(t)

	// Create test resources
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-a",
			},
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.test.com",
					VolumeHandle: "vol-123",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "default",
				Name:      "test-pvc",
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending, // Pod is not ready
		},
	}

	f.nodeLister = append(f.nodeLister, node)
	f.pvLister = append(f.pvLister, pv)
	f.pvcLister = append(f.pvcLister, pvc)
	f.podLister = append(f.podLister, pod)

	f.kubeObjects = append(f.kubeObjects, node, pv, pvc, pod)

	// No actions expected since pod is not ready

	// Run the test
	f.run("default/test-pod")
}

func TestDoesNotUpdatePVIfZoneLabelAlreadyCorrect(t *testing.T) {
	f := newFixture(t)

	// Create test resources
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-a",
			},
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-a", // Zone label already set correctly
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.test.com",
					VolumeHandle: "vol-123",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "default",
				Name:      "test-pvc",
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	f.nodeLister = append(f.nodeLister, node)
	f.pvLister = append(f.pvLister, pv)
	f.pvcLister = append(f.pvcLister, pvc)
	f.podLister = append(f.podLister, pod)

	f.kubeObjects = append(f.kubeObjects, node, pv, pvc, pod)

	// No actions expected since PV already has correct zone label

	// Run the test
	f.run("default/test-pod")
}

func TestUpdatesPVIfZoneLabelDifferent(t *testing.T) {
	f := newFixture(t)

	// Create test resources
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-a",
			},
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-b", // Different zone label
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.test.com",
					VolumeHandle: "vol-123",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "default",
				Name:      "test-pvc",
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	f.nodeLister = append(f.nodeLister, node)
	f.pvLister = append(f.pvLister, pv)
	f.pvcLister = append(f.pvcLister, pvc)
	f.podLister = append(f.podLister, pod)

	f.kubeObjects = append(f.kubeObjects, node, pv, pvc, pod)

	// Expected update to PV with correct zone label
	updatedPV := pv.DeepCopy()
	updatedPV.Labels[TopologyZoneLabel] = "us-west1-a"

	f.kubeActions = append(f.kubeActions, core.NewUpdateAction(
		schema.GroupVersionResource{Resource: "persistentvolumes"},
		"",
		updatedPV))

	// Run the test
	f.run("default/test-pod")
}

func TestProcessPVCWithReadyPod(t *testing.T) {
	f := newFixture(t)

	// Create test resources
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-a",
			},
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.test.com",
					VolumeHandle: "vol-123",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "default",
				Name:      "test-pvc",
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	f.nodeLister = append(f.nodeLister, node)
	f.pvLister = append(f.pvLister, pv)
	f.pvcLister = append(f.pvcLister, pvc)
	f.podLister = append(f.podLister, pod)

	f.kubeObjects = append(f.kubeObjects, node, pv, pvc, pod)

	// Expected update to PV with zone label
	updatedPV := pv.DeepCopy()
	if updatedPV.Labels == nil {
		updatedPV.Labels = make(map[string]string)
	}
	updatedPV.Labels[TopologyZoneLabel] = "us-west1-a"

	f.kubeActions = append(f.kubeActions, core.NewUpdateAction(
		schema.GroupVersionResource{Resource: "persistentvolumes"},
		"",
		updatedPV))

	// Run the test for PVC
	f.run("default/test-pvc")
}
