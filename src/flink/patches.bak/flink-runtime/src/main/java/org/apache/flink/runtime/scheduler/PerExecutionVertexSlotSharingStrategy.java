/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A slot sharing strategy that creates one execution slot sharing group per execution vertex.
 */
class PerExecutionVertexSlotSharingStrategy extends AbstractSlotSharingStrategy {

    PerExecutionVertexSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> logicalSlotSharingGroups,
            final Set<org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup> coLocationGroups) {
        super(topology, logicalSlotSharingGroups, coLocationGroups);
    }

    @Override
    protected Map<ExecutionVertexID, ExecutionSlotSharingGroup> computeExecutionSlotSharingGroups(
            SchedulingTopology schedulingTopology) {

        final Map<ExecutionVertexID, ExecutionSlotSharingGroup> map = new HashMap<>();

        // Build lookup from JobVertexID -> original logical SlotSharingGroup (if present)
        final Map<JobVertexID, SlotSharingGroup> originalSlotSharingGroupMap = new HashMap<>();
        for (SlotSharingGroup group : logicalSlotSharingGroups) {
            for (JobVertexID jId : group.getJobVertexIds()) {
                originalSlotSharingGroupMap.put(jId, group);
            }
        }

        for (SchedulingExecutionVertex schedulingExecutionVertex : schedulingTopology.getVertices()) {
            final ExecutionVertexID evId = schedulingExecutionVertex.getId();

            // create a fresh SlotSharingGroup for each execution vertex so groups are per-execution
            final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
            slotSharingGroup.addVertexToGroup(evId.getJobVertexId());

            // Preserve resource profile from the original logical group if present
            final SlotSharingGroup original = originalSlotSharingGroupMap.get(evId.getJobVertexId());
            final ResourceProfile resourceProfile =
                    original != null ? original.getResourceProfile() : ResourceProfile.UNKNOWN;
            slotSharingGroup.setResourceProfile(resourceProfile);

            final ExecutionSlotSharingGroup essg = new ExecutionSlotSharingGroup(slotSharingGroup);
            essg.addVertex(evId);
            map.put(evId, essg);
        }

        return map;
    }

    static class Factory implements SlotSharingStrategy.Factory {

        @Override
        public SlotSharingStrategy create(
                SchedulingTopology topology,
                Set<SlotSharingGroup> logicalSlotSharingGroups,
                Set<org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup> coLocationGroups) {

            return new PerExecutionVertexSlotSharingStrategy(topology, logicalSlotSharingGroups, coLocationGroups);
        }
    }
}
