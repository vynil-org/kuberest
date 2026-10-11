---
layout: page
title: Installation
permalink: /install/
---

(TL;DR: `kubectl apply -k github.com/vynil-org/kuberest//deploy`)

Since this is a kubernetes operator, the installations steps are:
- first the CustomResourceDefinition
- then the operator controlling the ressources

Feel free to pick any of the installtions options for both.

### CRD


#### kubectl

```sh
kubectl apply -f deploy/crd/crd.yaml
```
#### kustomize

```sh
kubectl apply -k github.com/vynil-org/kuberest//deploy/crd
```

### Operator

#### kubectl

```sh
helm template charts/kuberest | kubectl apply -f -
kubectl wait --for=condition=available deploy/kuberest --timeout=30s
```

#### kustomize

```sh
kubectl create ns kuberest
kubectl apply -k github.com/vynil-org/kuberest//deploy/operator
kubectl wait -n kuberest --for=condition=available deploy/kuberest --timeout=30s
```

#### helm

```sh
helm repo add kuberest https://vynil-org.github.io/kuberest/
kubectl create ns kuberest
helm install kuberest/kuberest kuberest --namespace kuberest
kubectl wait -n kuberest --for=condition=available deploy/kuberest --timeout=30s
```
