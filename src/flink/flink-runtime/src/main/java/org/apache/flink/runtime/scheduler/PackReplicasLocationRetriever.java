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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A custom preferred locations retriever that attempts to pack all replicas (parallel subtasks) of
 * a single JobVertex onto the same TaskManager.
 *
 * <p>The logic is as follows: 1. For the first replica of a JobVertex that needs to be scheduled,
 * it delegates the location preference to the default Flink logic (respecting state and data
 * locality). 2. It "locks in" the chosen location for that JobVertex. 3. For all subsequent
 * replicas of the same JobVertex, it returns the locked-in location as the sole preference.
 */
@NotThreadSafe
public class PackReplicasLocationRetriever implements SyncPreferredLocationsRetriever {

    /**
     * The default retriever to fall back on for the first replica. This ensures we still benefit
     * from Flink's state/data locality for initial placement.
     */
    private final SyncPreferredLocationsRetriever defaultRetriever;

    /**
     * Tracks the chosen TaskManagerLocation for each JobVertexID. This map is the "memory" that
     * enforces the packing strategy.
     */
    private final Map<JobVertexID, TaskManagerLocation> assignedLocations = new HashMap<>();

    public PackReplicasLocationRetriever(SyncPreferredLocationsRetriever defaultRetriever) {
        this.defaultRetriever = defaultRetriever;
    }

    @Override
    public Collection<TaskManagerLocation> getPreferredLocations(
            ExecutionVertexID executionVertexId, Set<ExecutionVertexID> producersToIgnore) {

        final JobVertexID jobVertexId = executionVertexId.getJobVertexId();

        // Check if we have already assigned a location for this JobVertex
        if (assignedLocations.containsKey(jobVertexId)) {
            // If yes, return that location as the only preference.
            return Collections.singleton(assignedLocations.get(jobVertexId));
        } else {
            // This is the first replica of this JobVertex we're scheduling.
            // Let the default Flink logic determine the best location(s).
            final Collection<TaskManagerLocation> preferredLocations =
                    defaultRetriever.getPreferredLocations(executionVertexId, producersToIgnore);

            if (preferredLocations.isEmpty()) {
                // The default logic had no preference. We can't make a decision yet.
                return Collections.emptyList();
            } else {
                // Pick one location from the preferred list (e.g., the first one) and
                // "lock it in" for all other replicas of this JobVertex.
                final TaskManagerLocation chosenLocation = preferredLocations.iterator().next();
                assignedLocations.put(jobVertexId, chosenLocation);
                return Collections.singleton(chosenLocation);
            }
        }
    }
}
