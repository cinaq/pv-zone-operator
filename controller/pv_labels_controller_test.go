package controller

import (
	"context"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/diff"
	kubeinformers "k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
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

	// Create a shared informer factory for the tests
	// This is only used for compatibility with the test framework
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(f.kubeClient, 0)

	controller := &PVZoneController{
		kubeClient:   f.kubeClient,
		resyncPeriod: 10 * time.Minute,
	}

	// No need to add objects to informers since we're using direct API calls now

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

	// Clear previous actions
	f.kubeClient.ClearActions()

	// Extract namespace and name from the key
	namespace, name, err := cache.SplitMetaNamespaceKey(podName)
	if err != nil {
		f.t.Errorf("invalid resource key: %s", podName)
		return
	}

	// Get the pod or PVC and process it
	var processErr error
	if namespace != "" {
		// Try to get the pod
		pod, err := controller.kubeClient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err == nil {
			// It's a pod, process it
			processErr = controller.processPod(pod)
		} else {
			// Try to get the PVC - in a real polling scenario, we'd just process all pods
			// but for test compatibility we'll keep this logic
			pvc, err := controller.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), name, metav1.GetOptions{})
			if err == nil {
				// For PVC test cases, find a pod using this PVC and process it
				pods, err := controller.findPodsUsingPVC(pvc)
				if err == nil && len(pods) > 0 {
					for i := range pods {
						if isPodReady(&pods[i]) {
							processErr = controller.processPod(&pods[i])
							break
						}
					}
				}
			}
		}
	}

	if !expectError && processErr != nil {
		f.t.Errorf("error processing resource: %v", processErr)
	} else if expectError && processErr == nil {
		f.t.Error("expected error processing resource, got nil")
	}

	// Since we're now using direct API calls, we only care about the update actions
	// We'll filter out all the get/list actions and just check the updates
	var updateActions []core.Action
	for _, action := range f.kubeClient.Actions() {
		if action.GetVerb() == "update" {
			updateActions = append(updateActions, action)
		}
	}

	// Check that we have the expected number of update actions
	if len(f.kubeActions) != len(updateActions) {
		if len(updateActions) > 0 {
			f.t.Errorf("Expected %d update actions, got %d", len(f.kubeActions), len(updateActions))
		} else if len(f.kubeActions) > 0 {
			// If we expected updates but got none, that's an error
			f.t.Errorf("Expected %d update actions, got none", len(f.kubeActions))
		}
		return
	}

	// Check each update action
	for i, action := range updateActions {
		expectedAction := f.kubeActions[i]
		checkAction(expectedAction, action, f.t)
	}
}

// runPeriodicScan has been removed as we're using a more flexible approach in TestPeriodicScanAllPods

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

func TestFindPodsUsingPVC(t *testing.T) {
	f := newFixture(t)

	// Create test resources
	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc-1",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv-1",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	// Create pods using the PVC
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-1",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-1",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc-1",
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

	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-2",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-2",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc-1", // Same PVC as pod1
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

	// Create a pod with a different PVC
	pod3 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-3",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-3",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc-2", // Different PVC
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	// Create a pod with no PVC
	pod4 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-4",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-4",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	// Create a pod in a different namespace
	pod5 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-5",
			Namespace: "other-namespace",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-5",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc-1", // Same name as pvc1 but in different namespace
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	// Add objects to the fixture
	f.kubeObjects = append(f.kubeObjects, pvc1, pod1, pod2, pod3, pod4, pod5)

	// Create controller
	controller, _ := f.newController()

	// Test findPodsUsingPVC function
	pods, err := controller.findPodsUsingPVC(pvc1)
	if err != nil {
		t.Errorf("Error finding pods using PVC: %v", err)
	}

	// Verify the correct pods were found
	if len(pods) != 2 {
		t.Errorf("Expected 2 pods using PVC, got %d", len(pods))
	}

	// Create a map of pod names for easier verification
	podNames := make(map[string]bool)
	for _, pod := range pods {
		podNames[pod.Name] = true
	}

	// Check that pod1 and pod2 are in the result
	if !podNames["test-pod-1"] {
		t.Errorf("Expected pod test-pod-1 to be found")
	}
	if !podNames["test-pod-2"] {
		t.Errorf("Expected pod test-pod-2 to be found")
	}

	// Check that pod3, pod4, and pod5 are NOT in the result
	if podNames["test-pod-3"] {
		t.Errorf("Pod test-pod-3 should not be found (different PVC)")
	}
	if podNames["test-pod-4"] {
		t.Errorf("Pod test-pod-4 should not be found (no PVC)")
	}
	if podNames["test-pod-5"] {
		t.Errorf("Pod test-pod-5 should not be found (different namespace)")
	}
}

func TestPeriodicScanAllPods(t *testing.T) {
	f := newFixture(t)

	// Create test resources - Node 1 with Pod 1 and PVC 1
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node-1",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-a",
			},
		},
	}

	pv1 := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv-1",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.test.com",
					VolumeHandle: "vol-1",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "default",
				Name:      "test-pvc-1",
			},
		},
	}

	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc-1",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv-1",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-1",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-1",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc-1",
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

	// Create test resources - Node 2 with Pod 2 and PVC 2
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node-2",
			Labels: map[string]string{
				TopologyZoneLabel: "us-west1-b",
			},
		},
	}

	pv2 := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv-2",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "csi.test.com",
					VolumeHandle: "vol-2",
				},
			},
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "default",
				Name:      "test-pvc-2",
			},
		},
	}

	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc-2",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv-2",
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-2",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-2",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-2",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc-2",
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

	// Add a pod that's not ready - should be skipped
	podNotReady := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-not-ready",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node-1",
			Volumes: []corev1.Volume{
				{
					Name: "test-volume-3",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc-3",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	f.nodeLister = append(f.nodeLister, node1, node2)
	f.pvLister = append(f.pvLister, pv1, pv2)
	f.pvcLister = append(f.pvcLister, pvc1, pvc2)
	f.podLister = append(f.podLister, pod1, pod2, podNotReady)

	f.kubeObjects = append(f.kubeObjects, node1, node2, pv1, pv2, pvc1, pvc2, pod1, pod2, podNotReady)

	// Expected updates to PVs with zone labels
	updatedPV1 := pv1.DeepCopy()
	if updatedPV1.Labels == nil {
		updatedPV1.Labels = make(map[string]string)
	}
	updatedPV1.Labels[TopologyZoneLabel] = "us-west1-a"

	updatedPV2 := pv2.DeepCopy()
	if updatedPV2.Labels == nil {
		updatedPV2.Labels = make(map[string]string)
	}
	updatedPV2.Labels[TopologyZoneLabel] = "us-west1-b"

	// Add expected actions - both PVs should be updated, but we don't know the order
	// So we'll check them separately after running the test
	expectedPVs := map[string]*corev1.PersistentVolume{
		updatedPV1.Name: updatedPV1,
		updatedPV2.Name: updatedPV2,
	}

	// Run the periodic scan
	controller, _ := f.newController()

	// Clear previous actions
	f.kubeClient.ClearActions()

	// Run the periodic scan
	controller.periodicScanAllPods()

	// Filter out all non-update actions
	var updateActions []core.Action
	for _, action := range f.kubeClient.Actions() {
		if action.GetVerb() == "update" {
			updateActions = append(updateActions, action)
		}
	}

	// Verify we have exactly 2 update actions
	if len(updateActions) != 2 {
		f.t.Errorf("Expected 2 update actions, got %d", len(updateActions))
		return
	}

	// Check each action
	for _, action := range updateActions {
		// Verify it's an update action on persistentvolumes
		if !action.Matches("update", "persistentvolumes") {
			f.t.Errorf("Expected update action on persistentvolumes, got %s %s", action.GetVerb(), action.GetResource().Resource)
			continue
		}

		// Get the updated PV
		updateAction, ok := action.(core.UpdateAction)
		if !ok {
			f.t.Errorf("Action is not an UpdateAction: %T", action)
			continue
		}

		updatedPV, ok := updateAction.GetObject().(*corev1.PersistentVolume)
		if !ok {
			f.t.Errorf("Updated object is not a PV: %T", updateAction.GetObject())
			continue
		}

		// Check if it matches one of our expected PVs
		expectedPV, found := expectedPVs[updatedPV.Name]
		if !found {
			f.t.Errorf("Unexpected PV update: %s", updatedPV.Name)
			continue
		}

		// Check if the zone label is correct
		if updatedPV.Labels[TopologyZoneLabel] != expectedPV.Labels[TopologyZoneLabel] {
			f.t.Errorf("PV %s has wrong zone label: expected %s, got %s",
				updatedPV.Name,
				expectedPV.Labels[TopologyZoneLabel],
				updatedPV.Labels[TopologyZoneLabel])
		}

		// Remove this PV from the expected map to track that we've seen it
		delete(expectedPVs, updatedPV.Name)
	}

	// Make sure all expected PVs were updated
	if len(expectedPVs) > 0 {
		for name := range expectedPVs {
			f.t.Errorf("Expected PV %s was not updated", name)
		}
	}
}
