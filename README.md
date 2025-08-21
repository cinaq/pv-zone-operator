# PV Zone Operator

Kubernetes operator that automatically labels PersistentVolumes with topology information based on the node where the pod using the PV is running. This is needed in the scenario that CSI provisioner itself does not set these labels. This is the case in [OVH Cloud](https://www.ovhcloud.com/). Perhaps also applicable in other vendors or environments. 

## Overview

The PV Zone Operator watches for pods in the ready state that use PersistentVolumeClaims. When it finds such pods, it:

1. Gets the topology labels (`topology.kubernetes.io/zone` and `topology.kubernetes.io/region`) from the node where the pod is running
2. Finds the PersistentVolume bound to the pod's PVC
3. Labels the PV with the same topology labels

This ensures that PersistentVolumes are correctly labeled with their topology information, which is important for pod scheduling and data locality.

## Features

- Automatically labels PVs with both zone and region information
- Only processes pods in the ready state
- Only updates PV labels when necessary (avoids unnecessary API calls)
- Works with any storage type (CSI, cloud provider volumes, etc.)
- Periodically scans all pods to ensure consistent labeling

## Installation

### Deploy the operator

```bash
kubectl apply -f manifests/pv-zone-operator.yaml
```

This will deploy the operator in the `kube-system` namespace with the necessary RBAC permissions to watch pods, PVCs, PVs, and nodes, and to update PVs with topology labels.

## How it works

The operator runs as a controller that:

1. Periodically scans all pods in the cluster (default resync period: 60 minutes)
2. For each ready pod, checks if it uses any PVCs
3. For each PVC, finds the bound PV
4. Gets the zone and region labels from the node where the pod is running
5. Updates the PV with the topology labels if they're not already set correctly

## Configuration

The operator supports the following command-line flags:

- `--kubeconfig`: Path to a kubeconfig file (only required when running outside of a cluster)
- `--master`: The address of the Kubernetes API server (overrides any value in kubeconfig)
- `--resync-period`: The interval at which all pods are rescanned (default: 60 minutes)

## RBAC Permissions

The operator requires the following permissions:

- `get`, `list`, `watch` on pods
- `get`, `list`, `watch` on persistentvolumeclaims
- `get`, `list`, `watch` on nodes
- `get`, `list`, `watch`, `update`, `patch` on persistentvolumes

## Resource Requirements

The operator is lightweight and has the following resource requests and limits:

- CPU: 100m (request and limit)
- Memory: 128Mi (request and limit)

## Community, discussion, contribution, and support

Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/).

You can reach the maintainers of this project at:

- [Slack channel](https://slack.k8s.io/)
- [Mailing list](https://groups.google.com/forum/#!forum/kubernetes-sig-cloud-provider)

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).

[owners]: https://git.k8s.io/community/contributors/guide/owners.md
[Creative Commons 4.0]: https://git.k8s.io/website/LICENSE