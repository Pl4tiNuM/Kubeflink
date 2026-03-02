package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceDeclaration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.api.common.resources.ExternalResource;


/**
 * Loads custom TaskManager resource declarations from a CSV file.
 *
 * CSV format (affinity currently ignored):
 *
 *   id,cpu,memory,slots,affinity
 *   1,2,8g,2,node1
 *   2,4,16g,4,node2
 *   3,8,32g,8,node3
 *   4,1,4g,2,node4
 */
public final class KubeflinkCRDLoader {

    private static final Logger LOG =
            LoggerFactory.getLogger(KubeflinkCRDLoader.class);

    private static final String DEFAULT_PATH = "/tm_config/tms_config.csv";

    private KubeflinkCRDLoader() {}

    /**
     * Load a collection of ResourceDeclarations from /tm_configs.csv.
     *
     * If the file is missing or fails to parse, returns an empty collection so that
     * ActiveResourceManager can fall back to the default Flink behaviour.
     */
    public static Collection<ResourceDeclaration> loadFromDefaultPath(
            Configuration configuration) {

        if (!Files.exists(Paths.get(DEFAULT_PATH))) {
            LOG.info(
                    "No TM config CSV found at {}. Using default Flink resource declarations.",
                    DEFAULT_PATH);
            return Collections.emptyList();
        }

        LOG.info("Loading TM resource declarations from {}", DEFAULT_PATH);

        try {
            List<TmConfigRow> rows = loadTmConfigs(DEFAULT_PATH);
            if (rows.isEmpty()) {
                LOG.warn("TM config CSV at {} is empty after header. Using defaults.", DEFAULT_PATH);
                return Collections.emptyList();
            }
            return buildDeclarations(rows, configuration);
        } catch (IOException e) {
            LOG.error("Failed to load TM configs from {}: {}", DEFAULT_PATH, e.toString(), e);
            return Collections.emptyList();
        }
    }

    private static List<TmConfigRow> loadTmConfigs(String path) throws IOException {
        List<TmConfigRow> result = new ArrayList<>();

        try (BufferedReader br =
                    Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {

            // Skip header
            String line = br.readLine();
            if (line == null) {
                return result;
            }

            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length < 8) {
                    LOG.warn("Skipping malformed TM config line (expected 8 columns): {}", line);
                    continue;
                }

                int id = Integer.parseInt(parts[0].trim());
                double k8sCpu = Double.parseDouble(parts[1].trim());
                int taskHeapMB = Integer.parseInt(parts[2].trim());
                int taskOffHeapMB = Integer.parseInt(parts[3].trim());
                int networkMB = Integer.parseInt(parts[4].trim());
                int managedMB = Integer.parseInt(parts[5].trim());
                int flinkSlots = Integer.parseInt(parts[6].trim());
                String k8sAffinity = parts[7].trim();

                result.add(
                        new TmConfigRow(
                                id,
                                k8sCpu,
                                taskHeapMB,
                                taskOffHeapMB,
                                networkMB,
                                managedMB,
                                flinkSlots,
                                k8sAffinity));
            }
        }

        LOG.info("Loaded {} TM config rows from {}", result.size(), path);
        return result;
    }

    private static Collection<ResourceDeclaration> buildDeclarations(
            List<TmConfigRow> rows, Configuration configuration) {

        // Create one ResourceDeclaration per CSV row to ensure each TM is created independently
        // This allows each TM to have its own affinity settings applied correctly
        List<ResourceDeclaration> decls = new ArrayList<>();

        for (TmConfigRow row : rows) {
            WorkerResourceSpec spec = buildWorkerResourceSpecFromRow(row);

            LOG.info("Processing TM row {}: spec={}, affinity={}", row.id, spec, row.k8sAffinity);

            // Create exactly ONE TM for this row
            ResourceDeclaration decl =
                    new ResourceDeclaration(
                            spec,
                            1,  // Each row creates exactly one TM
                            Collections.<InstanceID>emptySet());

            decls.add(decl);
            LOG.info(
                    "Declared ResourceDeclaration #{}: spec={}, affinity={}, numNeeded=1",
                    row.id,
                    spec,
                    row.k8sAffinity);
        }

        LOG.info("Total ResourceDeclarations created: {}", decls.size());
        return decls;
    }

    private static WorkerResourceSpec buildWorkerResourceSpecFromRow(TmConfigRow row) {
        WorkerResourceSpec.Builder builder = new WorkerResourceSpec.Builder();
        builder
                .setCpuCores(row.k8sCpu)
                .setTaskHeapMemoryMB(row.taskHeapMB)
                .setTaskOffHeapMemoryMB(row.taskOffHeapMB)
                .setNetworkMemoryMB(row.networkMB)
                .setManagedMemoryMB(row.managedMB)
                .setNumSlots(row.flinkSlots);

        // Add a unique extended resource to differentiate TMs with identical specs
        // This ensures each CSV row creates a distinct ResourceDeclaration
        // The resource name includes both the TM ID and affinity to make it unique
        String uniqueResourceName = "kubeflink-tm-" + row.id + "-" + row.k8sAffinity.replace(".", "-");
        ExternalResource uniqueMarker = new ExternalResource(uniqueResourceName, 1);
        builder.setExtendedResource(uniqueMarker);

        LOG.info("Created WorkerResourceSpec for TM {} with unique marker: {}", row.id, uniqueResourceName);

        return builder.build();
    }

    private static MemorySize scaleMemory(MemorySize base, double scale) {
        long bytes = base.getBytes();
        long scaled = (long) Math.round(bytes * scale);
        return new MemorySize(scaled);
    }

    private static final class TmConfigRow {
        final int id;
        final double k8sCpu;
        final int taskHeapMB;
        final int taskOffHeapMB;
        final int networkMB;
        final int managedMB;
        final int flinkSlots;
        final String k8sAffinity;

        TmConfigRow(
                int id,
                double k8sCpu,
                int taskHeapMB,
                int taskOffHeapMB,
                int networkMB,
                int managedMB,
                int flinkSlots,
                String k8sAffinity) {
            this.id = id;
            this.k8sCpu = k8sCpu;
            this.taskHeapMB = taskHeapMB;
            this.taskOffHeapMB = taskOffHeapMB;
            this.networkMB = networkMB;
            this.managedMB = managedMB;
            this.flinkSlots = flinkSlots;
            this.k8sAffinity = k8sAffinity;
        }

        @Override
        public String toString() {
            return "TmConfigRow{" +
                    "id=" + id +
                    ", k8sCpu=" + k8sCpu +
                    ", taskHeapMB=" + taskHeapMB +
                    ", taskOffHeapMB=" + taskOffHeapMB +
                    ", networkMB=" + networkMB +
                    ", managedMB=" + managedMB +
                    ", flinkSlots=" + flinkSlots +
                    ", k8sAffinity='" + k8sAffinity + '\'' +
                    '}';
        }
    }

}