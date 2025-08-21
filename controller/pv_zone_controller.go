package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// TopologyZoneLabel is the label key for zone information
	TopologyZoneLabel = "topology.kubernetes.io/zone"
	// TopologyRegionLabel is the label key for region information
	TopologyRegionLabel = "topology.kubernetes.io/region"
	// ControllerName is the name of this controller
	ControllerName = "pv-zone-operator"
)

// PVZoneController is the controller implementation for labeling PVs with zone information
type PVZoneController struct {
	kubeClient kubernetes.Interface

	// resyncPeriod is the period for full resync of all pods
	resyncPeriod time.Duration
}

// NewPVZoneController creates a new PVZoneController
func NewPVZoneController(
	kubeClient kubernetes.Interface,
	resyncPeriod time.Duration,
) *PVZoneController {
	controller := &PVZoneController{
		kubeClient:   kubeClient,
		resyncPeriod: resyncPeriod,
	}

	return controller
}

// Run starts the controller and runs until stopCh is closed
func (c *PVZoneController) Run(stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting PV Zone controller")

	// Start periodic scanning of all pods
	go wait.Until(c.periodicScanAllPods, c.resyncPeriod, stopCh)
	klog.Infof("Started periodic pod scanner with period %v", c.resyncPeriod)

	<-stopCh
	klog.Info("Shutting down controller")

	return nil
}

// periodicScanAllPods scans all pods in the cluster and updates PVs with zone information
func (c *PVZoneController) periodicScanAllPods() {
	klog.Info("Starting periodic scan of all pods")

	// List all pods in the cluster
	podList, err := c.kubeClient.CoreV1().Pods(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Error listing pods: %v", err)
		return
	}

	klog.Infof("Processing %d pods in periodic scan", len(podList.Items))

	// Process each pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		// Skip if the pod is not in ready state
		if !isPodReady(pod) {
			continue
		}

		// Process the pod to update PVs with zone information
		if err := c.processPod(pod); err != nil {
			klog.Errorf("Error processing pod %s/%s: %v", pod.Namespace, pod.Name, err)
		}
	}

	klog.Info("Completed periodic scan of all pods")
}

// We no longer need worker or queue processing functions as we're using polling

// processPod processes a pod to find its PVCs and label the corresponding PVs
func (c *PVZoneController) processPod(pod *corev1.Pod) error {
	// Skip if the pod is not in ready state
	if !isPodReady(pod) {
		return nil
	}

	// Get the node to find the zone
	node, err := c.kubeClient.CoreV1().Nodes().Get(context.TODO(), pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting node %s: %v", pod.Spec.NodeName, err)
	}

	// Get the zone from the node
	zone, zoneOk := node.Labels[TopologyZoneLabel]
	if !zoneOk {
		klog.Warningf("Node %s does not have zone label %s", node.Name, TopologyZoneLabel)
	}

	// Get the region from the node
	region, regionOk := node.Labels[TopologyRegionLabel]
	if !regionOk {
		klog.Warningf("Node %s does not have region label %s", node.Name, TopologyRegionLabel)
	}

	// Skip if neither zone nor region is available
	if !zoneOk && !regionOk {
		return nil
	}

	// Process each volume in the pod
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvc, err := c.kubeClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(
				context.TODO(), volume.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("Error getting PVC %s/%s: %v", pod.Namespace, volume.PersistentVolumeClaim.ClaimName, err)
				continue
			}

			if pvc.Spec.VolumeName == "" {
				klog.Warningf("PVC %s/%s does not have a bound PV", pod.Namespace, pvc.Name)
				continue
			}

			// Get the PV
			pv, err := c.kubeClient.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("Error getting PV %s: %v", pvc.Spec.VolumeName, err)
				continue
			}

			// Update the PV with topology labels
			err = c.updatePVWithTopologyLabels(pv, zone, region)
			if err != nil {
				klog.Errorf("Error updating PV %s with topology labels: %v", pv.Name, err)
				continue
			}
		}
	}

	return nil
}

// processPVC processes a PVC to label the corresponding PV
func (c *PVZoneController) processPVC(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Spec.VolumeName == "" {
		klog.Warningf("PVC %s/%s does not have a bound PV", pvc.Namespace, pvc.Name)
		return nil
	}

	// Find pods using this PVC
	pods, err := c.findPodsUsingPVC(pvc)
	if err != nil {
		return err
	}

	// Find a ready pod using this PVC
	var readyPod *corev1.Pod
	for i := range pods {
		if isPodReady(&pods[i]) {
			readyPod = &pods[i]
			break
		}
	}

	if readyPod == nil {
		klog.Infof("No ready pods found using PVC %s/%s", pvc.Namespace, pvc.Name)
		return nil
	}

	// Get the node to find the zone
	node, err := c.kubeClient.CoreV1().Nodes().Get(context.TODO(), readyPod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting node %s: %v", readyPod.Spec.NodeName, err)
	}

	// Get the zone from the node
	zone, zoneOk := node.Labels[TopologyZoneLabel]
	if !zoneOk {
		klog.Warningf("Node %s does not have zone label %s", node.Name, TopologyZoneLabel)
	}

	// Get the region from the node
	region, regionOk := node.Labels[TopologyRegionLabel]
	if !regionOk {
		klog.Warningf("Node %s does not have region label %s", node.Name, TopologyRegionLabel)
	}

	// Skip if neither zone nor region is available
	if !zoneOk && !regionOk {
		return nil
	}

	// Get the PV
	pv, err := c.kubeClient.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting PV %s: %v", pvc.Spec.VolumeName, err)
	}

	// Update the PV with topology labels
	return c.updatePVWithTopologyLabels(pv, zone, region)
}

// findPodsUsingPVC finds all pods using the given PVC
func (c *PVZoneController) findPodsUsingPVC(pvc *corev1.PersistentVolumeClaim) ([]corev1.Pod, error) {
	podList, err := c.kubeClient.CoreV1().Pods(pvc.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error listing pods: %v", err)
	}

	var podsUsingPVC []corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
				podsUsingPVC = append(podsUsingPVC, *pod)
				break
			}
		}
	}

	return podsUsingPVC, nil
}

// updatePVWithTopologyLabels updates the PV with the zone and region labels
func (c *PVZoneController) updatePVWithTopologyLabels(pv *corev1.PersistentVolume, zone, region string) error {
	// Check if the PV already has the correct topology labels
	needsUpdate := false

	// Clone the PV to avoid modifying the cache
	pvCopy := pv.DeepCopy()

	// Initialize labels map if it doesn't exist
	if pvCopy.Labels == nil {
		pvCopy.Labels = make(map[string]string)
	}

	// Check and set the zone label if provided
	if zone != "" {
		if value, ok := pv.Labels[TopologyZoneLabel]; !ok || value != zone {
			pvCopy.Labels[TopologyZoneLabel] = zone
			needsUpdate = true
		}
	}

	// Check and set the region label if provided
	if region != "" {
		if value, ok := pv.Labels[TopologyRegionLabel]; !ok || value != region {
			pvCopy.Labels[TopologyRegionLabel] = region
			needsUpdate = true
		}
	}

	// If no update is needed, return early
	if !needsUpdate {
		klog.V(4).Infof("PV %s already has correct topology labels", pv.Name)
		return nil
	}

	// Update the PV
	_, err := c.kubeClient.CoreV1().PersistentVolumes().Update(context.TODO(), pvCopy, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating PV %s: %v", pv.Name, err)
	}

	klog.Infof("Successfully updated PV %s with topology labels", pv.Name)
	return nil
}

// isPodReady returns true if a pod is ready; false otherwise.
func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
