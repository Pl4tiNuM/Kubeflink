# Enabling InPlace Vertical Scaling for Kubernetes before v1.33

1. Configure the files (in the control plane nodes): /etc/kubernetes/manifests/kube-apiserver.yaml and /etc/kubernetes/manifests/kube-controller-manager.yaml to include the following flag under command section:

```yaml
- --feature-gates=InPlacePodVerticalScaling=true,ExpandInUsePersistentVolumes=true
```

2. Configure the kubelet on each node (in /var/lib/kubelet/config.yaml) to include:

```yaml
featureGates:
  InPlacePodVerticalScaling: true
```

3. Restart the kubelet service on each node:

```bash
sudo systemctl restart kubelet
```

## Executing the Vertical Scale Script

To run the vertical-scale script, follow these steps:

1. Make sure to export the config to KUBECONFIG:
   ```bash
   export KUBECONFIG=/path/to/your/kubeconfig
   ```

2. Execute the vertical-scale script with the desired parameters:
   ```bash
   ./vertical-scale.sh --namespace <namespace> --pod <pod-name>  --container <container-name> --cpu-request <new-cpu-request> --cpu-limit <new-cpu-limit> --memory-request <new-memory-request> --memory-limit <new-memory-limit>
   ```

   Replace `<namespace>`, `<pod-name>`, `<container-name>`, `<new-cpu-request>`, `<new-cpu-limit>`, `<new-memory-request>`, and `<new-memory-limit>` with your specific values.