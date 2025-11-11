## Patches applied to flink source code to support Kubeflink's operation

This folder includes all the file changes made to support Kubeflink's operation. The changes are made on top of Flink v2.1.0. Simply use the dockerfile to build the image and then use it when deploying a Flink application natively over Kubernetes.

```
docker built -t kubeflink .
```

### Changelog

- `KubernetesResourceManagerDriver.java`: Added support to load TM resource profiles from a `.csv` file. Resource profiles currently include "CPU" and "MEM" requests for kubernetes. These are passed to `KubernetesTaskManagerParameters` class.