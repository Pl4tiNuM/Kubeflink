# Patches applied to flink source code to support Kubeflink's operation

This folder includes all the file changes made to support Kubeflink's operation. The changes are made on top of Flink v2.1.0. Simply use the dockerfile to build the image and then use it when deploying a Flink application natively over Kubernetes.

```
docker built -t kubeflink .
```

Before deploying an application, we first have to create a configmap with the TM resource profile configuration:
```
kubectl create configmap tm-configmap --from-file=example_config.csv=$(pwd)/example_config.csv
```

The `csv` must have the following format:
```
id,k8s_cpu,jvm_task_heap,jvm_task_offheap,jvm_network,jvm_managed,flink_slots,k8s_affinity
1,2,2048,512,1024,2048,2,node1
2,4,4096,1024,2048,4096,4,node2
3,8,8192,2048,4096,8192,8,node3
4,1,1024,256,512,1024,1,node4
```

The configmap can be loaded to the TM pods by specifying a custom POD template when deploying your application (using the ``-Dkubernetes.pod-template-file.default` flag.)

The respective names should match between the configmap and the Pod's YAML template.

## Changelog

### Module: `flink-runtime`
- `ActiveResourceManager.java`: Hooks into the resource declaration flow to support Flink-level heterogeneous TMs. Calls `KubeflinkCRDLoader.loadFromDefaultPath(flinkConfig)` inside `declareResourceNeeded(...)`. If csv-based declarations are present, they override the default, homogeneous ResourceDeclarations before `checkResourceDeclarations()` runs, so the RM requests multiple WorkerResourceSpec types instead of a single one.

- `KubeflinkCRDLoader.java`: New helper that loads heterogeneous TM specs from a CSV file at `/tm_config/tms_config.csv`. Parses the rows of the `csv` and for each row builds a `WorkerResourceSpec`. Returns a `Collection<ResourceDeclaration>` consumed by ActiveResourceManager to drive heterogeneous TMs allocation.

### Module: `flink-kubernetes`
- `KubernetesResourceManagerDriver.java`: Adds Kubeflink’s CSV-driven TM configuration logic. Loads per-TM settings from `/tm_config/tms_config.csv` and injects them into the TaskManager creation path. Overrides CPU, memory components (heap/offheap/network/managed), and slot count based on the CSV. The overrides are passed into `KubernetesTaskManagerParameters` for use when constructing the actual Kubernetes pod.

- `KubernetesTaskManagerParameters.java`: Extends the parameters object to support per-pod CPU, memory, and slot overrides. Adds fields for `overrideCpuLimit`, `overrideMemLimit`, and `overrideNumSlots`, and updates getters so that K8s resource requests/limits and Flink slot counts reflect the heterogeneous configuration.

- `CmdTaskManagerDecorator.java`: Updated to use the overridden CPU, memory, and slot values exposed via `KubernetesTaskManagerParameters`, ensuring that the generated K8s pod spec (container resources, env, launch parameters) matches the heterogeneous specifications defined in the CSV.