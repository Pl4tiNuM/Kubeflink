# Guide to add custom features on Flink's source code

1. Clone the repository. The `flink` folder includes the native source code of Flink v2.1.0.

2. To make any changes to the source code, first, we have to specify the source file that has to be changed. Let's take the `flink/flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/active/ActiveResourceManager.java` as an example.

3. First thing to do is copy the file on the `src/flink/patches` folder using the relative project's path. For example:
```
cp flink/flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/active/ActiveResourceManager.java src/flink/patches/flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/active/ActiveResourceManager.java
```

4. Ideally push the native source file on the repo before making any changes, so that you can later see the commit history on Github.
```
# Example
git add src/flink/patches/flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/active/ActiveResourceManager.java
git commit -m "Flink's default ActiveResourceManager.java"
git push -u origin main
```

5. Make any changes you want to the `ActiveResourceManager.java` file. Ideally also add a Logger and print stuff with "[Kubeflink]" string in front, so to easily check log files later.

6. Build the new docker image. By default the `Dockerfile` inside `src/flink/Dockerfile` copies all the files found under `patches` in the respective folder, replacing the default source files.
```
cd src/flink
docker build -t username/custom-flink .
...
docker push username/custom-flink
```

7. To test the changes, login to the Kubernetes master node and run a dummy workload. In the deployment flags, make sure you use 
```
ssh @master
cd flink/flink-dist/target/flink-2.1.0-bin/flink-2.1.0/
./bin/flink run --target kubernetes-application \
    -Dkubernetes.cluster-id=flink \
    -Dkubernetes.container.image.ref=custom-flink \
    -Dkubernetes.container.image.pull-policy=Always \
    -Dkubernetes.taskmanager.replicas=4 \
    -Dparallelism.default=4 \
    -Dkubernetes.cluster.persist-on-exception=true \
    -Dkubernetes.cluster.persist.deployment=true \
    -Dresourcemanager.taskmanager-timeout=100 \
    -Dkubernetes.rest-service.exposed.type=ClusterIP \
    -Dkubernetes.pod-template-file.default=/home/tzeneto/pod_template.yaml \
    -Denv.log.dir=/var/log/flink \
    -Dkubernetes.flink.log.dir=/var/log/flink \
    local:///opt/flink/examples/streaming/WordCount.jar --input file:///data/input.txt --output file:////data/output.txt
```

8. Wait for the JobManager to get `Running`:
```
watch kubectl get pods
```

9. Check the logs of the JobManager for any errors etc
```
kubectl logs <jm_pod_name>
```