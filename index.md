---
# Feel free to add content and custom Front Matter to this file.
# To modify the layout, see https://jekyllrb.com/docs/themes/#overriding-theme-defaults

layout: home
title: Home
---
# Configure applications and their relationship from kubernetes
![logo](./kuberest_logo.png "KubeRest")
![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)
![Kubernetes](https://img.shields.io/badge/kubernetes-%23326ce5.svg?style=for-the-badge&logo=kubernetes&logoColor=white)

[![Apache licensed](https://img.shields.io/badge/license-Apache-blue.svg)](./LICENSE)
[![ci](https://github.com/vynil-org/kuberest/actions/workflows/ci.yml/badge.svg)](https://github.com/vynil-org/kuberest/actions/workflows/ci.yml)
[![container image](https://img.shields.io/badge/ghcr.io-vynil--org%2Fkuberest-blue?logo=github)](
https://github.com/vynil-org/kuberest/pkgs/container/kuberest)


This repository contains a custom Kubernetes controller that can create/update/delete REST object on RESTfull api-endpoint.
The main goal is to not write a post-install Job filled with curl commands ever again for my applications deployments on Kubernetes. Inspirations come from the excellent [restapi terraform provider](https://registry.terraform.io/providers/Mastercard/restapi/latest/docs) and [Tekton](https://tekton.dev/docs/pipelines/).

## Use cases

- Configure OpenID applications in most ID-provider to provide seamless integration between applications
- Configure your own forge projects
- Configure any application that provide REST endpoints (that's a lot of them)

## Installation

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
kubectl apply -k github.com/vynil-org/kuberest//deploy/operator
```

#### helm

```sh
helm repo add kuberest https://vynil-org.github.io/kuberest/
kubectl create ns kuberest
helm install kuberest/kuberest kuberest --namespace kuberest
```

### Tenant aware

The controller can either function per-namespace (refuse to read secrets from other namespace mostly) or behave globally. The default behaviour is to limit to current namespace, to activate, set the environement variable MULTI_TENANT to false (or tenants.enabled=false for the chart)
