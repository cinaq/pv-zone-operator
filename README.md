# PV Labels Operator

[![CI](https://github.com/cinaq/pv-labels-operator/actions/workflows/ci.yaml/badge.svg)](https://github.com/cinaq/pv-labels-operator/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/cinaq/pv-labels-operator/branch/main/graph/badge.svg)](https://codecov.io/gh/cinaq/pv-labels-operator)

Kubernetes [operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) that automatically labels [PersistentVolumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) with topology information based on the node where the pod using the PV is running. This is needed in the scenario that [CSI](https://kubernetes.io/docs/concepts/storage/volumes/#csi) provisioner itself does not set these labels. This is the case in [OVH Cloud](https://www.ovhcloud.com/). Perhaps also applicable in other vendors or environments. 

The [topology labels](https://kubernetes.io/docs/concepts/storage/storage-classes/#volume-binding-mode) are important for Kubernetes scheduler to schedule pods in the right zone. Otherwise pods get deadlocked while waiting for a volume to mount that will never succeed.

## Overview

The PV Labels Operator watches for pods in the ready state that use PersistentVolumeClaims. When it finds such pods, it:

1. Gets the topology labels ([`topology.kubernetes.io/zone`](https://kubernetes.io/docs/reference/labels-annotations-taints/#topologykubernetesiozone) and [`topology.kubernetes.io/region`](https://kubernetes.io/docs/reference/labels-annotations-taints/#topologykubernetesioregion)) from the node where the pod is running
2. Finds the [PersistentVolume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) bound to the pod's [PVC](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims)
3. Labels the PV with the same topology labels

This ensures that PersistentVolumes are correctly labeled with their topology information, which is important for pod scheduling and data locality.

## Features

- Automatically labels PVs with both [zone and region information](https://kubernetes.io/docs/reference/labels-annotations-taints/#topologykubernetesiozone)
- Only processes pods in the [ready state](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-conditions)
- Only updates PV labels when necessary (avoids unnecessary API calls)
- Works with any storage type ([CSI](https://kubernetes.io/docs/concepts/storage/volumes/#csi), [cloud provider volumes](https://kubernetes.io/docs/concepts/storage/storage-classes/#provisioner), etc.)
- Periodically scans all pods to ensure consistent labeling

## Installation

### Deploy the operator

```bash
kubectl apply -f manifests/pv-labels-operator.yaml
```

This will deploy the operator in the `kube-system` namespace with the necessary [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) permissions to watch [pods](https://kubernetes.io/docs/concepts/workloads/pods/), [PVCs](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims), [PVs](https://kubernetes.io/docs/concepts/storage/persistent-volumes/), and [nodes](https://kubernetes.io/docs/concepts/architecture/nodes/), and to update PVs with topology labels.

## How it works

The operator runs as a [controller](https://kubernetes.io/docs/concepts/architecture/controller/) that:

1. Periodically scans all pods in the cluster (default resync period: 60 minutes)
2. For each ready pod, checks if it uses any [PVCs](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims)
3. For each PVC, finds the bound PV
4. Gets the [zone and region labels](https://kubernetes.io/docs/reference/labels-annotations-taints/#topologykubernetesiozone) from the node where the pod is running
5. Updates the PV with the topology labels if they're not already set correctly

## Configuration

The operator supports the following command-line flags:

- `--kubeconfig`: Path to a [kubeconfig file](https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/) (only required when running outside of a cluster)
- `--master`: The address of the [Kubernetes API server](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-apiserver/) (overrides any value in kubeconfig)
- `--resync-period`: The interval at which all pods are rescanned (default: 60 minutes)

## RBAC Permissions

The operator requires the following [RBAC permissions](https://kubernetes.io/docs/reference/access-authn-authz/rbac/):

- `get`, `list`, `watch` on [pods](https://kubernetes.io/docs/concepts/workloads/pods/)
- `get`, `list`, `watch` on [persistentvolumeclaims](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims)
- `get`, `list`, `watch` on [nodes](https://kubernetes.io/docs/concepts/architecture/nodes/)
- `get`, `list`, `watch`, `update`, `patch` on [persistentvolumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)

## Resource Requirements

The operator is lightweight and has the following resource requests and limits:

- CPU: 100m (request and limit)
- Memory: 128Mi (request and limit)

## Community, discussion, contribution, and support

Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/).

You can reach the maintainers of this project at:

- [Kubernetes Slack](https://slack.k8s.io/) - join the [#sig-storage channel](https://kubernetes.slack.com/messages/sig-storage)
- [SIG Storage Mailing list](https://groups.google.com/forum/#!forum/kubernetes-sig-storage)
- [SIG Cloud Provider Mailing list](https://groups.google.com/forum/#!forum/kubernetes-sig-cloud-provider)

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).

### Contributing

If you're interested in contributing to this project, please see the [CONTRIBUTING.md](CONTRIBUTING.md) file.

## CI/CD Workflows

This project uses GitHub Actions for continuous integration and deployment:

- **CI Workflow**: Runs tests and builds Docker images for the main branch and PRs targeting main.
- **Feature Branch Workflow**: Runs tests and builds Docker images for all other branches and PRs.
- **Release Workflow**: Creates GitHub releases and publishes Docker images when a new tag is pushed.

[owners]: https://git.k8s.io/community/contributors/guide/owners.md
[Creative Commons 4.0]: https://git.k8s.io/website/LICENSE