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

import java.util.Collection
import java.util.Collections
import java.util.IdentityHashMap
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.function.Consumer

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.operator.DataflowEventAdapter
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.extension.CH
import nextflow.processor.TaskProcessor

/**
 * Refines operator-backed Path demand using Nextflow/GPars processor closure
 * without interpreting operator names or values. Both GC modes consume this
 * detection; only their liveness clocks differ in GcArtifactRegistry.
 *
 * Concrete use remains owned by TaskRun reservations. This tracker proves only
 * that an operator-backed route can no longer create future demand.
 */
@Slf4j
@CompileStatic
final class GcRuntimeDemandTracker {

    private final GcProcessGraph graph
    private final GcArtifactRegistry registry
    private final Consumer<Collection<GcArtifactRegistry.DeletionResult>> deletionSink
    private final Map<GcProcessGraph.OutputPort, Set<DataflowProcessor>> pendingByPort = new LinkedHashMap<>()
    private final Map<DataflowProcessor, Set<GcProcessGraph.OutputPort>> portsByProcessor = new IdentityHashMap<>()
    private final Set<DataflowProcessor> listenedProcessors = newIdentityProcessorSet()

    GcRuntimeDemandTracker(
        GcProcessGraph graph,
        GcArtifactRegistry registry,
        Consumer<Collection<GcArtifactRegistry.DeletionResult>> deletionSink
    ) {
        if( graph == null || registry == null )
            throw new IllegalArgumentException('Runtime demand tracker dependencies must not be null')
        this.graph = graph
        this.registry = registry
        this.deletionSink = deletionSink
    }

    /** Attach before Session ignites the dataflow network. */
    void attach() {
        for( TaskProcessor producer : graph.processes ) {
            for( GcProcessGraph.OutputPort port : graph.outputPortsOf(producer) ) {
                if( graph.routeKind(port) != GcProcessGraph.PortRouteKind.FALLBACK )
                    continue

                /*
                 * A value channel feeding a process can be reused by later task
                 * instances, so it stays on coarse fallback. An operator-only
                 * route has no such process demand; closing its observed operator
                 * processors is the only exact proof that the Path is no longer
                 * used.
                 */
                if( CH.isValue(port.channel) && !graph.consumersOf(port).isEmpty() )
                    continue

                final Set<DataflowProcessor> processors = graph.runtimeClosureProcessorsOf(port)
                if( processors.isEmpty() )
                    continue

                final Set<DataflowProcessor> pending = newIdentityProcessorSet()
                pending.addAll(processors)
                pendingByPort.put(port, pending)

                for( DataflowProcessor processor : processors ) {
                    Set<GcProcessGraph.OutputPort> ports = portsByProcessor.get(processor)
                    if( ports == null ) {
                        ports = new LinkedHashSet<GcProcessGraph.OutputPort>()
                        portsByProcessor.put(processor, ports)
                    }
                    ports.add(port)
                    attachProcessor(processor)
                }
            }
        }
    }

    private void attachProcessor(DataflowProcessor processor) {
        if( processor == null || !listenedProcessors.add(processor) )
            return

        processor.addDataflowEventListener(new DataflowEventAdapter() {
            @Override
            void afterStop(DataflowProcessor stopped) {
                try {
                    onProcessorStopped(stopped)
                }
                catch( Exception e ) {
                    log.warn 'nf-gc runtime demand observer failed while sealing an operator-backed route; retaining artifacts conservatively', e
                }
            }
        })
    }

    private synchronized void onProcessorStopped(DataflowProcessor processor) {
        final Set<GcProcessGraph.OutputPort> ports = portsByProcessor.get(processor)
        if( ports == null )
            return

        for( GcProcessGraph.OutputPort port : ports ) {
            final Set<DataflowProcessor> pending = pendingByPort.get(port)
            if( pending == null || !pending.remove(processor) || !pending.isEmpty() )
                continue

            final List<GcArtifactRegistry.DeletionResult> deletions = registry.onRuntimeRouteSealed(port)
            if( deletionSink != null && !deletions.isEmpty() )
                deletionSink.accept(deletions)
        }
    }

    private static Set<DataflowProcessor> newIdentityProcessorSet() {
        return Collections.newSetFromMap(new IdentityHashMap<DataflowProcessor,Boolean>())
    }
}
