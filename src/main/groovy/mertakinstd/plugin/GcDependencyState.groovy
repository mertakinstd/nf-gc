/*
 * Copyright 2026, Mert Akin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mertakinstd.plugin

import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.IdentityHashMap
import java.util.List
import java.util.Set

import groovy.transform.CompileStatic
import nextflow.processor.TaskProcessor

/**
 * Tracks process-level dependency closure for shallow GC.
 *
 * A process is dependency-closed only after the process itself and every
 * immediate consumer in the process graph have terminated. Dependency closure
 * is deliberately weaker than GC eligibility: ownership, publication and
 * artifact-safety checks are handled by later stages.
 */
@CompileStatic
final class GcDependencyState {

    private final GcProcessGraph graph
    private final Set<TaskProcessor> terminated = newIdentityProcessSet()
    private final Set<TaskProcessor> closed = newIdentityProcessSet()

    GcDependencyState(GcProcessGraph graph) {
        if( graph == null )
            throw new IllegalArgumentException('Process graph must not be null')
        this.graph = graph
    }

    /**
     * Marks a process terminated and returns any processes that became
     * dependency-closed as a consequence.
     *
     * Unknown processes are conservatively ignored: UNKNOWN -> KEEP.
     */
    Collection<TaskProcessor> onProcessTerminate(TaskProcessor process) {
        if( process == null || !graph.contains(process) )
            return Collections.emptyList()

        terminated.add(process)

        final List<TaskProcessor> candidates = new ArrayList<>()
        candidates.add(process)
        candidates.addAll(graph.producersOf(process))

        final List<TaskProcessor> newlyClosed = new ArrayList<>()
        for( TaskProcessor candidate : candidates ) {
            if( closed.contains(candidate) || !terminated.contains(candidate) )
                continue
            if( !allConsumersTerminated(candidate) )
                continue

            closed.add(candidate)
            newlyClosed.add(candidate)
        }

        return Collections.unmodifiableList(newlyClosed)
    }

    boolean isTerminated(TaskProcessor process) {
        return process != null && terminated.contains(process)
    }

    boolean isDependencyClosed(TaskProcessor process) {
        return process != null && closed.contains(process)
    }

    private boolean allConsumersTerminated(TaskProcessor process) {
        for( TaskProcessor consumer : graph.consumersOf(process) ) {
            if( !terminated.contains(consumer) )
                return false
        }
        return true
    }

    private static Set<TaskProcessor> newIdentityProcessSet() {
        return Collections.newSetFromMap(new IdentityHashMap<TaskProcessor, Boolean>())
    }
}
