package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	// TopologyZoneLabel is the label key for zone information
	TopologyZoneLabel = "topology.kubernetes.io/zone"
	// TopologyRegionLabel is the label key for region information
	TopologyRegionLabel = "topology.kubernetes.io/region"
	// ControllerName is the name of this controller
ControllerName = "pv-labels-operator"
)

// PVLabelsController is the controller implementation for labeling PVs with topology information
type PVLabelsController struct {
	kubeClient kubernetes.Interface

	// resyncPeriod is the period for full resync of all pods
	resyncPeriod time.Duration

	// Informers and listers
	podInformer cache.SharedIndexInformer
	podLister   v1.PodLister
	podsSynced  cache.InformerSynced

	// workqueue is a rate limited work queue that handles pod events
	workqueue workqueue.RateLimitingInterface
}

// NewPVLabelsController creates a new PVLabelsController
func NewPVLabelsController(
	kubeClient kubernetes.Interface,
	resyncPeriod time.Duration,
) *PVLabelsController {
	// Create pod informer
	podInformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (apimachruntime.Object, error) {
				return kubeClient.CoreV1().Pods(metav1.NamespaceAll).List(context.TODO(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return kubeClient.CoreV1().Pods(metav1.NamespaceAll).Watch(context.TODO(), options)
			},
		},
		&corev1.Pod{},
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	controller := &PVLabelsController{
		kubeClient:   kubeClient,
		resyncPeriod: resyncPeriod,
		podInformer:  podInformer,
		podLister:    v1.NewPodLister(podInformer.GetIndexer()),
		podsSynced:   podInformer.HasSynced,
		workqueue:    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "PVLabelsController"),
	}

	// Set up event handlers for pod informer
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePod,
		UpdateFunc: func(old, new interface{}) {
			oldPod := old.(*corev1.Pod)
			newPod := new.(*corev1.Pod)
			// Only process if pod transitions to Ready state
			if !isPodReady(oldPod) && isPodReady(newPod) {
				controller.enqueuePod(newPod)
			}
		},
		// We don't need to handle delete events
	})

	return controller
}

// Run starts the controller and runs until stopCh is closed
func (c *PVLabelsController) Run(stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting PV Labels controller")

	// Start the pod informer
	go c.podInformer.Run(stopCh)

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(stopCh, c.podsSynced) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Start the worker to process pod events
	go wait.Until(c.runWorker, time.Second, stopCh)
	klog.Info("Started workers")

	// Also keep the periodic scanning as a fallback mechanism
	go wait.Until(c.periodicScanAllPods, c.resyncPeriod, stopCh)
	klog.Infof("Started periodic pod scanner with period %v", c.resyncPeriod)

	<-stopCh
	klog.Info("Shutting down controller")

	return nil
}

// periodicScanAllPods scans all pods in the cluster and updates PVs with topology information
func (c *PVLabelsController) periodicScanAllPods() {
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

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the workqueue.
func (c *PVLabelsController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the processPod method.
func (c *PVLabelsController) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the processPodByKey, passing it the namespace/name string of the Pod resource to be processed
		if err := c.processPodByKey(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error processing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		klog.Infof("Successfully processed '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// enqueuePod takes a Pod resource and converts it into a namespace/name
// string which is then put onto the work queue.
func (c *PVLabelsController) enqueuePod(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

// processPodByKey processes a pod from the key in the workqueue
func (c *PVLabelsController) processPodByKey(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("invalid resource key: %s", key)
	}

	// Get the Pod resource with this namespace/name
	pod, err := c.podLister.Pods(namespace).Get(name)
	if err != nil {
		// The Pod resource may no longer exist, in which case we stop processing
		return nil
	}

	// Process the pod to update PVs with zone information
	return c.processPod(pod)
}

// processPod processes a pod to find its PVCs and label the corresponding PVs
func (c *PVLabelsController) processPod(pod *corev1.Pod) error {
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
func (c *PVLabelsController) processPVC(pvc *corev1.PersistentVolumeClaim) error {
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
func (c *PVLabelsController) findPodsUsingPVC(pvc *corev1.PersistentVolumeClaim) ([]corev1.Pod, error) {
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
func (c *PVLabelsController) updatePVWithTopologyLabels(pv *corev1.PersistentVolume, zone, region string) error {
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
