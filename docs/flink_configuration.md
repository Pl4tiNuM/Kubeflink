# Flink Configuration Flags for Kubernetes Native Deployment

Flink over Kubernetes native deployment allows us to set specific configurations (e.g., number of task managers) by specifying them using `-D` flag during deployment:
```
./bin/flink run --target kubernetes-application -Dkubernetes.taskmanager.replicas=2
```

The complete configuration list can be found in the link below:
https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/

## Important configuration flags
Below are some important flags for resource allocation and logging:

### Resource allocation
`kubernetes.taskmanager.replicas`: Number of Task Managers to deploy for the specific application.


### Logging
`kubernetes.flink.log.dir`: The directory that logs of jobmanager and taskmanager be saved in the pod.
