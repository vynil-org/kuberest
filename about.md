---
layout: page
title: About
permalink: /about/
---

![logo](../kuberest_logo.png "KubeRest")
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
