/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerDriverConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesTooOldResourceVersionException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.util.ResourceManagerUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import io.fabric8.kubernetes.client.KubernetesClientException;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.flink.configuration.MemorySize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Kubeflink Imports */

/** Implementation of {@link ResourceManagerDriver} for Kubernetes deployment. */
public class KubernetesResourceManagerDriver
        extends AbstractResourceManagerDriver<KubernetesWorkerNode> {

    /** The taskmanager pod name pattern is {clusterId}-{taskmanager}-{attemptId}-{podIndex}. */
    private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";

    private static final Long TERMINATION_WAIT_SECOND = 5L;

    private final String clusterId;

    private final String webInterfaceUrl;

    private final FlinkKubeClient flinkKubeClient;

    /** Request resource futures, keyed by pod names. */
    private final Map<String, CompletableFuture<KubernetesWorkerNode>> requestResourceFutures;

    /** When ResourceManager failover, the max attempt should recover. */
    private long currentMaxAttemptId = 0;

    /** Current max pod index. When creating a new pod, it should increase one. */
    private long currentMaxPodId = 0;

    private CompletableFuture<KubernetesWatch> podsWatchOptFuture =
            FutureUtils.completedExceptionally(
                    new ResourceManagerException(
                            "KubernetesResourceManagerDriver is not initialized."));

    private volatile boolean running;

    private FlinkPod taskManagerPodTemplate;

    /* Kubeflink start */
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceManagerDriver.class);
    private static final String TM_CONFIG_PATH = "/tm_config/tms_config.csv";

    private static Map<Integer, Map<String, String>> loadTMConfig() { 
        final Map<Integer, Map<String, String>> config = new HashMap<>();
        final List<String> lines;

        try {
            LOG.info("[Kubeflink] Loading TM config from {}", TM_CONFIG_PATH);
            lines = Files.readAllLines(Paths.get(TM_CONFIG_PATH));
        } catch (IOException e) {
            LOG.warn("[Kubeflink] Failed to read TM config file", e);
            throw new RuntimeException("[Kubeflink] Failed to read TM config file", e);
        }

        for (int i = 0; i < lines.size(); i++) {
            String raw = lines.get(i);

            // Skip header
            if (i == 0) {
                LOG.info("[Kubeflink] Skipping header: {}", raw);
                continue;
            }
            if (raw == null) {
                continue;
            }

            final String line = raw.trim();

            // Skip empty lines and comments
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            final String[] parts = line.split(",");
            if (parts.length < 8) {
                LOG.warn(
                        "[Kubeflink] Skipping malformed TM config line (expected 8 columns, got {}): {}",
                        parts.length,
                        line);
                continue;
            }

            final int id = Integer.parseInt(parts[0].trim());
            final Map<String, String> entry = new HashMap<>();

            final String k8sCpu       = parts[1].trim();
            final String jvmTaskHeap  = parts[2].trim();
            final String jvmTaskOff   = parts[3].trim();
            final String jvmNetwork   = parts[4].trim();
            final String jvmManaged   = parts[5].trim();
            final String flinkSlots   = parts[6].trim();
            final String k8sAffinity  = parts[7].trim();

            // New descriptive keys
            entry.put("k8s_cpu",        k8sCpu);
            entry.put("jvm_task_heap",  jvmTaskHeap);
            entry.put("jvm_task_offheap", jvmTaskOff);
            entry.put("jvm_network",    jvmNetwork);
            entry.put("jvm_managed",    jvmManaged);
            entry.put("flink_slots",    flinkSlots);
            entry.put("k8s_affinity",   k8sAffinity);

            // Optional legacy aliases for existing code
            entry.put("cpu",   k8sCpu);
            entry.put("slots", flinkSlots);
            // no single "mem" anymore – you should switch callers to use the
            // jvm_* fields instead of "mem"

            config.put(id, entry);

            LOG.info(
                    "[Kubeflink] Loaded TM config id={} k8s_cpu={} heap={} offheap={} network={} managed={} slots={} affinity={}",
                    id,
                    k8sCpu,
                    jvmTaskHeap,
                    jvmTaskOff,
                    jvmNetwork,
                    jvmManaged,
                    flinkSlots,
                    k8sAffinity);
        }

        return config;
    } 
    /* Kubeflink End */


    public KubernetesResourceManagerDriver(
            Configuration flinkConfig,
            FlinkKubeClient flinkKubeClient,
            KubernetesResourceManagerDriverConfiguration configuration) {
        super(flinkConfig, GlobalConfiguration.loadConfiguration());
        this.clusterId = Preconditions.checkNotNull(configuration.getClusterId());
        this.webInterfaceUrl = configuration.getWebInterfaceUrl();
        this.flinkKubeClient = Preconditions.checkNotNull(flinkKubeClient);
        this.requestResourceFutures = new HashMap<>();
        this.running = false;
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerDriver
    // ------------------------------------------------------------------------

    @Override
    protected void initializeInternal() throws Exception {
        podsWatchOptFuture = watchTaskManagerPods();
        final File podTemplateFile = KubernetesUtils.getTaskManagerPodTemplateFileInPod();
        if (podTemplateFile.exists()) {
            taskManagerPodTemplate =
                    KubernetesUtils.loadPodFromTemplateFile(
                            flinkKubeClient, podTemplateFile, Constants.MAIN_CONTAINER_NAME);
        } else {
            taskManagerPodTemplate = new FlinkPod.Builder().build();
        }
        updateKubernetesServiceTargetPortIfNecessary();
        recoverWorkerNodesFromPreviousAttempts();
        this.running = true;
    }

    @Override
    public void terminate() throws Exception {
        if (!running) {
            return;
        }
        running = false;

        // shut down all components
        Exception exception = null;

        try {
            podsWatchOptFuture.get(TERMINATION_WAIT_SECOND, TimeUnit.SECONDS).close();
        } catch (Exception e) {
            exception = e;
        }

        try {
            flinkKubeClient.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void deregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
        log.info(
                "Deregistering Flink Kubernetes cluster, clusterId: {}, diagnostics: {}",
                clusterId,
                optionalDiagnostics == null ? "" : optionalDiagnostics);
        flinkKubeClient.stopAndCleanupCluster(clusterId);
    }

    @Override
    public CompletableFuture<KubernetesWorkerNode> requestResource(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        final KubernetesTaskManagerParameters parameters =
                createKubernetesTaskManagerParameters(
                        taskExecutorProcessSpec, getBlockedNodeRetriever().getAllBlockedNodeIds());
        final KubernetesPod taskManagerPod =
                KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(
                        taskManagerPodTemplate, parameters);
        final String podName = taskManagerPod.getName();
        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                new CompletableFuture<>();

        requestResourceFutures.put(podName, requestResourceFuture);

        log.info(
                "Creating new TaskManager pod with name {} and resource <{},{}>.",
                podName,
                parameters.getTaskManagerMemoryMB(),
                parameters.getTaskManagerCPU());

        final CompletableFuture<Void> createPodFuture =
                flinkKubeClient.createTaskManagerPod(taskManagerPod);

        FutureUtils.assertNoException(
                createPodFuture.handleAsync(
                        (ignore, exception) -> {
                            if (exception != null) {
                                log.warn(
                                        "Could not create pod {}, exception: {}",
                                        podName,
                                        exception);
                                CompletableFuture<KubernetesWorkerNode> future =
                                        requestResourceFutures.remove(taskManagerPod.getName());
                                if (future != null) {
                                    future.completeExceptionally(exception);
                                }
                            } else {
                                if (requestResourceFuture.isCancelled()) {
                                    stopPod(podName);
                                    log.info(
                                            "pod {} is cancelled before create pod finish, stop it.",
                                            podName);
                                } else {
                                    log.info("Pod {} is created.", podName);
                                }
                            }
                            return null;
                        },
                        getMainThreadExecutor()));

        FutureUtils.assertNoException(
                requestResourceFuture.handle(
                        (ignore, t) -> {
                            if (t == null) {
                                return null;
                            }
                            // Unwrap CompletionException cause if any
                            if (t instanceof CompletionException && t.getCause() != null) {
                                t = t.getCause();
                            }
                            if (t instanceof CancellationException) {

                                requestResourceFutures.remove(taskManagerPod.getName());
                                if (createPodFuture.isDone()) {
                                    log.info(
                                            "pod {} is cancelled before scheduled, stop it.",
                                            podName);
                                    stopPod(taskManagerPod.getName());
                                }
                            } else if (t instanceof RetryableException
                                    || t instanceof KubernetesClientException) {
                                // ignore transient / retriable errors
                            } else {
                                log.error("Error completing resource request.", t);
                                ExceptionUtils.rethrow(t);
                            }
                            return null;
                        }));

        return requestResourceFuture;
    }

    @Override
    public void releaseResource(KubernetesWorkerNode worker) {
        final String podName = worker.getResourceID().toString();

        log.info("Stopping TaskManager pod {}.", podName);

        stopPod(podName);
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
        List<KubernetesPod> podList =
                flinkKubeClient.getPodsWithLabels(
                        KubernetesUtils.getTaskManagerSelectors(clusterId));
        final List<KubernetesWorkerNode> recoveredWorkers = new ArrayList<>();

        for (KubernetesPod pod : podList) {
            final KubernetesWorkerNode worker =
                    new KubernetesWorkerNode(new ResourceID(pod.getName()));
            final long attempt = worker.getAttempt();
            if (attempt > currentMaxAttemptId) {
                currentMaxAttemptId = attempt;
            }

            if (pod.isTerminated() || !pod.isScheduled()) {
                stopPod(pod.getName());
            } else {
                recoveredWorkers.add(worker);
            }
        }

        log.info(
                "Recovered {} pods from previous attempts, current attempt id is {}.",
                recoveredWorkers.size(),
                ++currentMaxAttemptId);

        getResourceEventHandler().onPreviousAttemptWorkersRecovered(recoveredWorkers);
    }

    private void updateKubernetesServiceTargetPortIfNecessary() throws Exception {
        if (!KubernetesUtils.isHostNetwork(flinkConfig)) {
            return;
        }
        final int restPort =
                ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl(webInterfaceUrl);
        Preconditions.checkArgument(
                restPort > 0, "Failed to parse rest port from " + webInterfaceUrl);
        final String restServiceName = ExternalServiceDecorator.getExternalServiceName(clusterId);
        flinkKubeClient
                .updateServiceTargetPort(restServiceName, Constants.REST_PORT_NAME, restPort)
                .get();
        if (!HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig)) {
            final String internalServiceName =
                    InternalServiceDecorator.getInternalServiceName(clusterId);
            flinkKubeClient
                    .updateServiceTargetPort(
                            internalServiceName,
                            Constants.BLOB_SERVER_PORT_NAME,
                            Integer.parseInt(flinkConfig.get(BlobServerOptions.PORT)))
                    .get();
            flinkKubeClient
                    .updateServiceTargetPort(
                            internalServiceName,
                            Constants.JOB_MANAGER_RPC_PORT_NAME,
                            flinkConfig.get(JobManagerOptions.PORT))
                    .get();
        }
    }

    private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(
            TaskExecutorProcessSpec taskExecutorProcessSpec, Set<String> blockedNodes) {
        final String podName =
                String.format(
                        TASK_MANAGER_POD_FORMAT, clusterId, currentMaxAttemptId, ++currentMaxPodId);

        /* Kubeflink Start */
        log.info("[Kubeflink] Creating TM Pod with ID {}", currentMaxPodId);

        final Map<Integer, Map<String, String>> tmConfig = loadTMConfig();
        final Map<String, String> current_config = tmConfig.get((int) currentMaxPodId);

        if (current_config == null) {
            throw new RuntimeException(
                    String.format(
                            "[Kubeflink] No TM config found for id=%d in %s",
                            currentMaxPodId, TM_CONFIG_PATH));
        }

        // New CSV fields
        final double overrideCpu =
                Double.parseDouble(current_config.get("k8s_cpu"));
        final int taskHeapMB =
                Integer.parseInt(current_config.get("jvm_task_heap"));
        final int taskOffHeapMB =
                Integer.parseInt(current_config.get("jvm_task_offheap"));
        final int networkMB =
                Integer.parseInt(current_config.get("jvm_network"));
        final int managedMB =
                Integer.parseInt(current_config.get("jvm_managed"));
        final int overrideNumSlots =
                Integer.parseInt(current_config.get("flink_slots"));
        final String nodeAffinity =
                current_config.get("k8s_affinity");

        // Convert MB → bytes → MemorySize
        final MemorySize tmHeapMemory =
                new MemorySize((long) taskHeapMB * 1024L * 1024L);
        final MemorySize tmOffHeapMemory =
                new MemorySize((long) taskOffHeapMB * 1024L * 1024L);
        final MemorySize tmNetworkMemory =
                new MemorySize((long) networkMB * 1024L * 1024L);
        final MemorySize tmManagedMemory =
                new MemorySize((long) managedMB * 1024L * 1024L);

        // Total TM memory used as the "k8s memory" override (sum of all components)
        final long totalBytes =
                tmHeapMemory.getBytes()
                        + tmOffHeapMemory.getBytes()
                        + tmNetworkMemory.getBytes()
                        + tmManagedMemory.getBytes();
        final MemorySize overrideMem = new MemorySize(totalBytes);

        log.info(
                "[Kubeflink] podName={} CPU={} heap={} offheap={} network={} managed={} slots={} affinity={}",
                podName,
                overrideCpu,
                tmHeapMemory,
                tmOffHeapMemory,
                tmNetworkMemory,
                tmManagedMemory,
                overrideNumSlots,
                nodeAffinity);
        /* Kubeflink End */

        final ContaineredTaskManagerParameters taskManagerParameters =
                ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

        final Configuration taskManagerConfig = new Configuration(flinkConfig);

        log.info("[Kubeflink] Original TM Config: {}", taskManagerConfig.toMap().toString());
        taskManagerConfig.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, podName);

        /* Kubeflink Start */
        // CPU + slots override
        taskManagerConfig.set(TaskManagerOptions.CPU_CORES, overrideCpu);
        taskManagerConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, overrideNumSlots);

        // Fine-grained memory from CSV
        taskManagerConfig.set(TaskManagerOptions.TASK_HEAP_MEMORY, tmHeapMemory);
        taskManagerConfig.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, tmOffHeapMemory);
        taskManagerConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, tmManagedMemory);
        taskManagerConfig.set(TaskManagerOptions.NETWORK_MEMORY_MIN, tmNetworkMemory);
        taskManagerConfig.set(TaskManagerOptions.NETWORK_MEMORY_MAX, tmNetworkMemory);
        /* Kubeflink End */

        log.info("[Kubeflink] Final TM Config: {}", taskManagerConfig.toMap().toString());

        final String dynamicProperties =
                BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);
        log.info("[Kubeflink] Dynamic Properties: {}", dynamicProperties);

        String jvmMemOpts =
                ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec);

        return new KubernetesTaskManagerParameters(
                taskManagerConfig,
                podName,
                dynamicProperties,
                jvmMemOpts,
                taskManagerParameters,
                ExternalResourceUtils.getExternalResourceConfigurationKeys(
                        flinkConfig,
                        KubernetesConfigOptions
                                .EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX),
                blockedNodes,
                overrideCpu,    // Kubeflink: K8s CPU limit/request
                overrideMem,    // Kubeflink: total K8s memory
                overrideNumSlots); // Kubeflink: TM slots
    }


    private void handlePodEventsInMainThread(List<KubernetesPod> pods, PodEvent podEvent) {
        getMainThreadExecutor()
                .execute(
                        () -> {
                            for (KubernetesPod pod : pods) {
                                // we should also handle the deleted event to avoid situations where
                                // the pod itself doesn't reflect the status correctly (i.e. pod
                                // removed during the pending phase).
                                if (podEvent == PodEvent.DELETED || pod.isTerminated()) {
                                    onPodTerminated(pod);
                                } else if (pod.isScheduled()) {
                                    onPodScheduled(pod);
                                }
                            }
                        });
    }

    private void onPodScheduled(KubernetesPod pod) {
        final String podName = pod.getName();
        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                requestResourceFutures.remove(podName);

        if (requestResourceFuture == null) {
            log.debug("Ignore TaskManager pod that is already added: {}", podName);
            return;
        }

        log.info("Received new TaskManager pod: {}", podName);
        requestResourceFuture.complete(new KubernetesWorkerNode(new ResourceID(podName)));
    }

    private void onPodTerminated(KubernetesPod pod) {
        final String podName = pod.getName();
        log.debug("TaskManager pod {} is terminated.", podName);

        // this is a safe net, in case onModified/onDeleted/onError is
        // received before onAdded
        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                requestResourceFutures.remove(podName);
        if (requestResourceFuture != null) {
            log.warn("Pod {} is terminated before being scheduled.", podName);
            requestResourceFuture.completeExceptionally(
                    new RetryableException("Pod is terminated."));
        }

        getResourceEventHandler()
                .onWorkerTerminated(new ResourceID(podName), pod.getTerminatedDiagnostics());
        stopPod(podName);
    }

    private void stopPod(String podName) {
        flinkKubeClient
                .stopPod(podName)
                .whenComplete(
                        (ignore, throwable) -> {
                            if (throwable != null) {
                                log.warn(
                                        "Could not remove TaskManager pod {}, exception: {}",
                                        podName,
                                        throwable);
                            }
                        });
    }

    private CompletableFuture<KubernetesWatch> watchTaskManagerPods() throws Exception {
        CompletableFuture<KubernetesWatch> kubernetesWatchCompletableFuture =
                flinkKubeClient.watchPodsAndDoCallback(
                        KubernetesUtils.getTaskManagerSelectors(clusterId),
                        new PodCallbackHandlerImpl());
        kubernetesWatchCompletableFuture.whenCompleteAsync(
                (KubernetesWatch watch, Throwable throwable) -> {
                    if (throwable != null) {
                        getResourceEventHandler().onError(throwable);
                    } else {
                        log.info("Create watch on TaskManager pods successfully.");
                    }
                },
                getMainThreadExecutor());
        return kubernetesWatchCompletableFuture;
    }

    // ------------------------------------------------------------------------
    //  FlinkKubeClient.WatchCallbackHandler
    // ------------------------------------------------------------------------

    private class PodCallbackHandlerImpl
            implements FlinkKubeClient.WatchCallbackHandler<KubernetesPod> {
        @Override
        public void onAdded(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods, PodEvent.ADDED);
        }

        @Override
        public void onModified(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods, PodEvent.MODIFIED);
        }

        @Override
        public void onDeleted(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods, PodEvent.DELETED);
        }

        @Override
        public void onError(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods, PodEvent.ERROR);
        }

        @Override
        public void handleError(Throwable throwable) {
            if (throwable instanceof KubernetesTooOldResourceVersionException) {
                podsWatchOptFuture.whenCompleteAsync(
                        (KubernetesWatch watch, Throwable throwable1) -> {
                            if (running) {
                                try {
                                    if (watch != null) {
                                        watch.close();
                                    }
                                } catch (Exception e) {
                                    log.warn(
                                            "Error when get old watch to close, which is not supposed to happen",
                                            e);
                                }
                                log.info("Creating a new watch on TaskManager pods.");
                                try {
                                    podsWatchOptFuture = watchTaskManagerPods();
                                } catch (Exception e) {
                                    getResourceEventHandler().onError(e);
                                }
                            }
                        },
                        getMainThreadExecutor());
            } else {
                getResourceEventHandler().onError(throwable);
            }
        }
    }

    private static class RetryableException extends FlinkException {
        private static final long serialVersionUID = 1L;

        RetryableException(String message) {
            super(message);
        }
    }

    /** Internal type of the pod event. */
    private enum PodEvent {
        ADDED,
        MODIFIED,
        DELETED,
        ERROR
    }
}
