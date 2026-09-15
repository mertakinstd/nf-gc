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
import java.util.LinkedHashSet
import java.util.Set

import groovy.transform.CompileStatic
import nextflow.processor.TaskProcessor

/**
 * Tracks dependency closure for producer output ports.
 *
 * A non-terminal output port closes only after its producer process and every
 * immediate downstream consumer process reachable from that port have
 * terminated. This deliberately remains process-granular within a port; it
 * does not infer task-instance consumption or scheduling state.
 */
@CompileStatic
final class GcOutputDependencyState {

    private final GcProcessGraph graph
    private final GcDependencyState processState
    private final Set<GcProcessGraph.OutputPort> closed = new LinkedHashSet<>()

    GcOutputDependencyState(GcProcessGraph graph, GcDependencyState processState) {
        if( graph == null )
            throw new IllegalArgumentException('Process graph must not be null')
        if( processState == null )
            throw new IllegalArgumentException('Process dependency state must not be null')
        this.graph = graph
        this.processState = processState
    }

    /**
     * Re-evaluates only ports whose state can change when this process
     * terminates: ports produced by it and upstream ports consumed by it.
     */
    synchronized Collection<GcProcessGraph.OutputPort> onProcessTerminate(TaskProcessor process) {
        if( process == null || !graph.contains(process) )
            return Collections.emptyList()

        final Set<GcProcessGraph.OutputPort> candidates = new LinkedHashSet<>()
        candidates.addAll(graph.outputPortsOf(process))
        candidates.addAll(graph.inputPortsOf(process))

        final List<GcProcessGraph.OutputPort> newlyClosed = new ArrayList<>()
        for( GcProcessGraph.OutputPort port : candidates ) {
            if( closed.contains(port) || graph.isTerminal(port) )
                continue
            if( !processState.isTerminated(port.producer) )
                continue
            if( !allConsumersTerminated(port) )
                continue

            closed.add(port)
            newlyClosed.add(port)
        }
        return Collections.unmodifiableList(newlyClosed)
    }

    synchronized boolean isClosed(GcProcessGraph.OutputPort port) {
        return port != null && closed.contains(port)
    }

    private boolean allConsumersTerminated(GcProcessGraph.OutputPort port) {
        for( TaskProcessor consumer : graph.consumersOf(port) ) {
            if( !processState.isTerminated(consumer) )
                return false
        }
        return true
    }
}
