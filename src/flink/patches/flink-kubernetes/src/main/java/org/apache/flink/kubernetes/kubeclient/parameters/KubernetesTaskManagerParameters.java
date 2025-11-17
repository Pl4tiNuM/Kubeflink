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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.configuration.MemorySize;


/**
 * A utility class helps parse, verify, and manage the Kubernetes side parameters that are used for
 * constructing the TaskManager Pod.
 */
public class KubernetesTaskManagerParameters extends AbstractKubernetesParameters {

    private final String podName;

    private final String dynamicProperties;

    private final String jvmMemOptsEnv;

    private final ContaineredTaskManagerParameters containeredTaskManagerParameters;

    private final Map<String, String> taskManagerExternalResourceConfigKeys;

    private final Set<String> blockedNodes;

    private final Double overrideCpuLimit; // Kubeflink

    private final MemorySize overrideMemLimit; // Kubeflink

    private final Integer overrideNumSlots;   // Kubeflink


    public KubernetesTaskManagerParameters(
        Configuration flinkConfig,
        String podName,
        String dynamicProperties,
        String jvmMemOptsEnv,
        ContaineredTaskManagerParameters containeredTaskManagerParameters,
        Map<String, String> taskManagerExternalResourceConfigKeys,
        Set<String> blockedNodes) {

    this(
        flinkConfig,
        podName,
        dynamicProperties,
        jvmMemOptsEnv,
        containeredTaskManagerParameters,
        taskManagerExternalResourceConfigKeys,
        blockedNodes,
        null,   // overrideCpuLimit
        null,
        null    // overrideMemoryLimit
    );
    }

    public KubernetesTaskManagerParameters(
            Configuration flinkConfig,
            String podName,
            String dynamicProperties,
            String jvmMemOptsEnv,
            ContaineredTaskManagerParameters containeredTaskManagerParameters,
            Map<String, String> taskManagerExternalResourceConfigKeys,
            Set<String> blockedNodes,
            @Nullable Double overrideCpuLimit,
            @Nullable MemorySize overrideMemLimit,
            @Nullable Integer overrideNumSlots) {
        super(flinkConfig);
        this.podName = checkNotNull(podName);
        this.dynamicProperties = checkNotNull(dynamicProperties);
        this.jvmMemOptsEnv = checkNotNull(jvmMemOptsEnv);
        this.containeredTaskManagerParameters = checkNotNull(containeredTaskManagerParameters);
        this.taskManagerExternalResourceConfigKeys =
                checkNotNull(taskManagerExternalResourceConfigKeys);
        this.blockedNodes = checkNotNull(blockedNodes);
        this.overrideCpuLimit = overrideCpuLimit;
        this.overrideMemLimit = overrideMemLimit;
        this.overrideNumSlots = overrideNumSlots;
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.TASK_MANAGER_LABELS)
                        .orElse(Collections.emptyMap()));
        labels.putAll(getSelectors());
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public Map<String, String> getSelectors() {
        return KubernetesUtils.getTaskManagerSelectors(getClusterId());
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return Collections.unmodifiableMap(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR)
                        .orElse(Collections.emptyMap()));
    }

    @Override
    public Map<String, String> getEnvironments() {
        return this.containeredTaskManagerParameters.taskManagerEnv();
    }

    @Override
    public Map<String, String> getAnnotations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS)
                .orElse(Collections.emptyMap());
    }

    @Override
    public List<Map<String, String>> getTolerations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS)
                .orElse(Collections.emptyList());
    }

    public String getPodName() {
        return podName;
    }

    public int getTaskManagerMemoryMB() {
        if (overrideMemLimit != null) {
            return (int) overrideMemLimit.getMebiBytes();
        }
        return containeredTaskManagerParameters
                .getTaskExecutorProcessSpec()
                .getTotalProcessMemorySize()
                .getMebiBytes();
    }

    public double getTaskManagerCPU() {
        if (overrideCpuLimit != null) {
            return overrideCpuLimit;
        }
        return containeredTaskManagerParameters
                .getTaskExecutorProcessSpec()
                .getCpuCores()
                .getValue()
                .doubleValue();
    }

    public int getTaskManagerSlots() {
        if (overrideNumSlots != null) {
            return overrideNumSlots;
        }
        // fall back to whatever Flink would normally use
        return containeredTaskManagerParameters
                .getTaskExecutorProcessSpec()
                .getNumSlots();
    }

    public double getTaskManagerCPULimitFactor() {
        final double limitFactor =
                flinkConfig.get(KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR);
        checkArgument(
                limitFactor >= 1,
                "%s should be greater or equal to 1.",
                KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR.key());
        return limitFactor;
    }

    public double getTaskManagerMemoryLimitFactor() {
        final double limitFactor =
                flinkConfig.get(KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR);
        checkArgument(
                limitFactor >= 1,
                "%s should be greater or equal to 1.",
                KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR.key());
        return limitFactor;
    }

    public Map<String, ExternalResource> getTaskManagerExternalResources() {
        return containeredTaskManagerParameters.getTaskExecutorProcessSpec().getExtendedResources();
    }

    public String getServiceAccount() {
        return flinkConfig.get(KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT);
    }

    public Map<String, String> getTaskManagerExternalResourceConfigKeys() {
        return Collections.unmodifiableMap(taskManagerExternalResourceConfigKeys);
    }

    public int getRPCPort() {
        final int taskManagerRpcPort =
                KubernetesUtils.parsePort(flinkConfig, TaskManagerOptions.RPC_PORT);
        checkArgument(
                taskManagerRpcPort > 0, "%s should not be 0.", TaskManagerOptions.RPC_PORT.key());
        return taskManagerRpcPort;
    }


    public String getDynamicProperties() {
        return dynamicProperties;
    }

    @Nullable
    public Integer getOverrideNumSlots() {
        return overrideNumSlots;
    }

    public String getJvmMemOptsEnv() {
        return jvmMemOptsEnv;
    }

    public ContaineredTaskManagerParameters getContaineredTaskManagerParameters() {
        return containeredTaskManagerParameters;
    }

    public Set<String> getBlockedNodes() {
        return Collections.unmodifiableSet(blockedNodes);
    }

    public String getNodeNameLabel() {
        return checkNotNull(flinkConfig.get(KubernetesConfigOptions.KUBERNETES_NODE_NAME_LABEL));
    }

    public String getEntrypointArgs() {
        return flinkConfig.get(KubernetesConfigOptions.KUBERNETES_TASKMANAGER_ENTRYPOINT_ARGS);
    }
}
