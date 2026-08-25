#!/bin/bash

. scripts/common.sh

# Fix kvm permission on kind
$RUNTIME exec -ti kind-control-plane chmod 666 /dev/kvm
export VERSION=$(curl -s https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-operator.yaml"
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-cr.yaml"

kubectl patch kubevirt kubevirt -n kubevirt --type='merge' -p \
'{"spec":{"configuration":{"developerConfiguration":{"featureGates":["ExperimentalIgnitionSupport"]},"ksmConfiguration":{"nodeLabelSelector":{}}}}}'

kubectl wait --for=jsonpath='{.status.phase}'=Deployed kubevirt/kubevirt -n kubevirt --timeout=15m

# KubeVirt v1.7+ omits the client-cert volume mount from the virt-handler
# DaemonSet, causing repeated "failed to load certificate
# /etc/virt-handler/clientcertificates/tls.crt" errors that prevent VM
# scheduling. Patch the DaemonSet and let the new pod pick up the secret.
if ! kubectl -n kubevirt get daemonset virt-handler -o json \
    | grep -q '"mountPath":"/etc/virt-handler/clientcertificates"'; then
  kubectl -n kubevirt patch daemonset virt-handler --type='json' -p='[
    {"op":"add","path":"/spec/template/spec/volumes/-","value":{"name":"kubevirt-virt-handler-certs","secret":{"secretName":"kubevirt-virt-handler-certs","optional":true}}},
    {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"kubevirt-virt-handler-certs","mountPath":"/etc/virt-handler/clientcertificates","readOnly":true}}
  ]'
  kubectl -n kubevirt rollout status daemonset/virt-handler --timeout=120s
fi
