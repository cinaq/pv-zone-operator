package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	// TopologyZoneLabel is the label key for zone information
	TopologyZoneLabel = "topology.kubernetes.io/zone"
	// ControllerName is the name of this controller
	ControllerName = "pv-zone-operator"
)

// PVZoneController is the controller implementation for labeling PVs with zone information
type PVZoneController struct {
	kubeClient kubernetes.Interface

	podLister       corelisters.PodLister
	podListerSynced cache.InformerSynced

	pvLister       corelisters.PersistentVolumeLister
	pvListerSynced cache.InformerSynced

	pvcLister       corelisters.PersistentVolumeClaimLister
	pvcListerSynced cache.InformerSynced

	nodeLister       corelisters.NodeLister
	nodeListerSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens.
	workqueue workqueue.RateLimitingInterface
}

// NewPVZoneController creates a new PVZoneController
func NewPVZoneController(
	kubeClient kubernetes.Interface,
	resyncPeriod time.Duration,
) *PVZoneController {
	// Create informer factories
	informerFactory := coreinformers.NewFilteredPodInformer(
		kubeClient,
		metav1.NamespaceAll,
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		nil,
	)

	pvInformer := coreinformers.NewFilteredPersistentVolumeInformer(
		kubeClient,
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		nil,
	)

	pvcInformer := coreinformers.NewFilteredPersistentVolumeClaimInformer(
		kubeClient,
		metav1.NamespaceAll,
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		nil,
	)

	nodeInformer := coreinformers.NewFilteredNodeInformer(
		kubeClient,
		resyncPeriod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		nil,
	)

	controller := &PVZoneController{
		kubeClient: kubeClient,
		workqueue:  workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), ControllerName),
	}

	klog.Info("Setting up event handlers")
	// Set up event handlers for pod changes
	informerFactory.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePod,
		UpdateFunc: func(old, new interface{}) {
			newPod := new.(*corev1.Pod)
			oldPod := old.(*corev1.Pod)
			if newPod.ResourceVersion == oldPod.ResourceVersion {
				// Periodic resync will send update events for all known Pods.
				// Two different versions of the same Pod will always have different RVs.
				return
			}
			controller.enqueuePod(new)
		},
	})

	// Set up event handlers for PVC changes
	pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePVC,
		UpdateFunc: func(old, new interface{}) {
			newPVC := new.(*corev1.PersistentVolumeClaim)
			oldPVC := old.(*corev1.PersistentVolumeClaim)
			if newPVC.ResourceVersion == oldPVC.ResourceVersion {
				return
			}
			controller.enqueuePVC(new)
		},
	})

	controller.podLister = corelisters.NewPodLister(informerFactory.GetIndexer())
	controller.podListerSynced = informerFactory.HasSynced

	controller.pvLister = corelisters.NewPersistentVolumeLister(pvInformer.GetIndexer())
	controller.pvListerSynced = pvInformer.HasSynced

	controller.pvcLister = corelisters.NewPersistentVolumeClaimLister(pvcInformer.GetIndexer())
	controller.pvcListerSynced = pvcInformer.HasSynced

	controller.nodeLister = corelisters.NewNodeLister(nodeInformer.GetIndexer())
	controller.nodeListerSynced = nodeInformer.HasSynced

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *PVZoneController) Run(workers int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting PV Zone controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podListerSynced, c.pvListerSynced, c.pvcListerSynced, c.nodeListerSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch workers to process Pod resources
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *PVZoneController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *PVZoneController) processNextWorkItem() bool {
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
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Pod resource to be synced.
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Pod resource
// with the current status of the resource.
func (c *PVZoneController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Check if this is a pod or PVC key
	if namespace != "" {
		// This is a namespaced resource, likely a Pod or PVC
		// Try to get the Pod first
		pod, err := c.podLister.Pods(namespace).Get(name)
		if err == nil {
			// It's a pod, process it
			return c.processPod(pod)
		} else if !errors.IsNotFound(err) {
			return err
		}

		// Try to get the PVC
		pvc, err := c.pvcLister.PersistentVolumeClaims(namespace).Get(name)
		if err == nil {
			// It's a PVC, process it
			return c.processPVC(pvc)
		} else if !errors.IsNotFound(err) {
			return err
		}
	}

	// Not found or not a resource we care about
	return nil
}

// enqueuePod takes a Pod resource and converts it into a namespace/name
// string which is then put onto the work queue.
func (c *PVZoneController) enqueuePod(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

// enqueuePVC takes a PVC resource and converts it into a namespace/name
// string which is then put onto the work queue.
func (c *PVZoneController) enqueuePVC(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

// processPod processes a pod to find its PVCs and label the corresponding PVs
func (c *PVZoneController) processPod(pod *corev1.Pod) error {
	// Skip if the pod is not in ready state
	if !isPodReady(pod) {
		return nil
	}

	// Get the node to find the zone
	node, err := c.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		return fmt.Errorf("error getting node %s: %v", pod.Spec.NodeName, err)
	}

	// Get the zone from the node
	zone, ok := node.Labels[TopologyZoneLabel]
	if !ok {
		klog.Warningf("Node %s does not have zone label %s", node.Name, TopologyZoneLabel)
		return nil
	}

	// Process each volume in the pod
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvc, err := c.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(volume.PersistentVolumeClaim.ClaimName)
			if err != nil {
				klog.Warningf("Error getting PVC %s/%s: %v", pod.Namespace, volume.PersistentVolumeClaim.ClaimName, err)
				continue
			}

			if pvc.Spec.VolumeName == "" {
				klog.Warningf("PVC %s/%s does not have a bound PV", pod.Namespace, pvc.Name)
				continue
			}

			// Get the PV
			pv, err := c.pvLister.Get(pvc.Spec.VolumeName)
			if err != nil {
				klog.Warningf("Error getting PV %s: %v", pvc.Spec.VolumeName, err)
				continue
			}

			// Update the PV with the zone label
			err = c.updatePVWithZoneLabel(pv, zone)
			if err != nil {
				klog.Errorf("Error updating PV %s with zone label: %v", pv.Name, err)
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
	for _, pod := range pods {
		if isPodReady(pod) {
			readyPod = pod
			break
		}
	}

	if readyPod == nil {
		klog.Infof("No ready pods found using PVC %s/%s", pvc.Namespace, pvc.Name)
		return nil
	}

	// Get the node to find the zone
	node, err := c.nodeLister.Get(readyPod.Spec.NodeName)
	if err != nil {
		return fmt.Errorf("error getting node %s: %v", readyPod.Spec.NodeName, err)
	}

	// Get the zone from the node
	zone, ok := node.Labels[TopologyZoneLabel]
	if !ok {
		klog.Warningf("Node %s does not have zone label %s", node.Name, TopologyZoneLabel)
		return nil
	}

	// Get the PV
	pv, err := c.pvLister.Get(pvc.Spec.VolumeName)
	if err != nil {
		return fmt.Errorf("error getting PV %s: %v", pvc.Spec.VolumeName, err)
	}

	// Update the PV with the zone label
	return c.updatePVWithZoneLabel(pv, zone)
}

// findPodsUsingPVC finds all pods using the given PVC
func (c *PVZoneController) findPodsUsingPVC(pvc *corev1.PersistentVolumeClaim) ([]*corev1.Pod, error) {
	pods, err := c.podLister.Pods(pvc.Namespace).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("error listing pods: %v", err)
	}

	var podsUsingPVC []*corev1.Pod
	for _, pod := range pods {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
				podsUsingPVC = append(podsUsingPVC, pod)
				break
			}
		}
	}

	return podsUsingPVC, nil
}

// updatePVWithZoneLabel updates the PV with the zone label
func (c *PVZoneController) updatePVWithZoneLabel(pv *corev1.PersistentVolume, zone string) error {
	// Check if the PV already has the correct zone label
	if value, ok := pv.Labels[TopologyZoneLabel]; ok && value == zone {
		klog.V(4).Infof("PV %s already has correct zone label %s=%s", pv.Name, TopologyZoneLabel, zone)
		return nil
	}

	// Clone the PV to avoid modifying the cache
	pvCopy := pv.DeepCopy()

	// Initialize labels map if it doesn't exist
	if pvCopy.Labels == nil {
		pvCopy.Labels = make(map[string]string)
	}

	// Set the zone label
	pvCopy.Labels[TopologyZoneLabel] = zone

	// Update the PV
	_, err := c.kubeClient.CoreV1().PersistentVolumes().Update(context.TODO(), pvCopy, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating PV %s: %v", pv.Name, err)
	}

	klog.Infof("Successfully updated PV %s with zone label %s=%s", pv.Name, TopologyZoneLabel, zone)
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
