#!/bin/bash

ps -e -o pid,exe | grep -P 'kube-controller-manager|kube-apiserver|etcd' | awk '{print $1}' | xargs -I{} kill {}