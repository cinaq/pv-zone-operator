# Kubernetes PV Labeler Operator

Kubernetes operator that automatically labels PersistentVolumes with zone information based on the node where the pod using the PV is running.

## Overview

The PV Zone Operator watches for pods in the ready state that use PersistentVolumeClaims. When it finds such pods, it:

1. Gets the zone label (`topology.kubernetes.io/zone`) from the node where the pod is running
2. Finds the PersistentVolume bound to the pod's PVC
3. Labels the PV with the same zone label

This ensures that PersistentVolumes are correctly labeled with their topology information, which is important for pod scheduling and data locality.

## Features

- Automatically labels PVs with the correct zone information
- Only processes pods in the ready state
- Only updates PV labels when necessary
- Works with any storage type (CSI, cloud provider volumes, etc.)

## Installation

### Deploy the operator

```bash
kubectl apply -f manifests/pv-zone-operator.yaml
```

This will deploy the operator with the necessary RBAC permissions to watch pods, PVCs, PVs, and nodes, and to update PVs with zone labels.

## How it works

The operator runs as a controller that:

1. Watches for pods that are in the ready state
2. For each ready pod, checks if it uses any PVCs
3. For each PVC, finds the bound PV
4. Gets the zone label from the node where the pod is running
5. Updates the PV with the zone label if it's not already set correctly

The operator also watches for PVC events to handle cases where a PVC is bound to a PV after the pod is already running.

## RBAC Permissions

The operator requires the following permissions:

- `get`, `list`, `watch` on pods
- `get`, `list`, `watch` on persistentvolumeclaims
- `get`, `list`, `watch` on nodes
- `get`, `list`, `watch`, `update`, `patch` on persistentvolumes

## Community, discussion, contribution, and support

Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/).

You can reach the maintainers of this project at:

- [Slack channel](https://slack.k8s.io/)
- [Mailing list](https://groups.google.com/forum/#!forum/kubernetes-sig-cloud-provider)

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).

[owners]: https://git.k8s.io/community/contributors/guide/owners.md
[Creative Commons 4.0]: https://git.k8s.io/website/LICENSE