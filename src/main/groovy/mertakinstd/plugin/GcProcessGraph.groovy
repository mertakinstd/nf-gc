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

import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Collections
import java.util.IdentityHashMap
import java.util.List
import java.util.Map
import java.util.Set

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import nextflow.dag.DAG
import nextflow.processor.TaskProcessor

/**
 * Process-level projection of Nextflow's workflow DAG.
 *
 * Paths through non-process vertices are collapsed until the first downstream
 * process vertex is reached. TaskProcessor identity is preserved with
 * reference-equality collections so lifecycle callbacks can be matched to the
 * exact process instances represented by the DAG.
 */
@CompileStatic
final class GcProcessGraph {

    private final Map<TaskProcessor, Set<TaskProcessor>> consumers
    private final Map<TaskProcessor, Set<TaskProcessor>> producers

    @PackageScope
    GcProcessGraph(
        Map<TaskProcessor, Set<TaskProcessor>> consumers,
        Map<TaskProcessor, Set<TaskProcessor>> producers
    ) {
        this.consumers = consumers
        this.producers = producers
    }

    static GcProcessGraph from(DAG dag) {
        if( dag == null )
            throw new IllegalArgumentException('Nextflow DAG must not be null')

        final Map<DAG.Vertex, List<DAG.Vertex>> outgoing = new IdentityHashMap<>()

        for( DAG.Vertex vertex : dag.vertices )
            outgoing.put(vertex, new ArrayList<DAG.Vertex>())

        for( DAG.Edge edge : dag.edges ) {
            final DAG.Vertex from = edge.from
            final DAG.Vertex to = edge.to

            // Dangling origin/sink edges are not process-to-process dependencies.
            if( from == null || to == null )
                continue

            List<DAG.Vertex> targets = outgoing.get(from)
            if( targets == null ) {
                targets = new ArrayList<DAG.Vertex>()
                outgoing.put(from, targets)
            }
            targets.add(to)
        }

        final Map<TaskProcessor, Set<TaskProcessor>> consumers = new IdentityHashMap<>()
        final Map<TaskProcessor, Set<TaskProcessor>> producers = new IdentityHashMap<>()

        for( DAG.Vertex vertex : dag.vertices ) {
            final TaskProcessor process = vertex.process
            if( process == null )
                continue

            consumers.put(process, findImmediateConsumers(vertex, outgoing))
            producers.put(process, newIdentityProcessSet())
        }

        for( Map.Entry<TaskProcessor, Set<TaskProcessor>> entry : consumers.entrySet() ) {
            final TaskProcessor producer = entry.key
            for( TaskProcessor consumer : entry.value ) {
                final Set<TaskProcessor> upstream = producers.get(consumer)
                if( upstream == null )
                    throw new IllegalStateException(
                        "DAG consumer is missing a process vertex: ${consumer.name}"
                    )
                upstream.add(producer)
            }
        }

        return new GcProcessGraph(consumers, producers)
    }

    Set<TaskProcessor> getProcesses() {
        return Collections.unmodifiableSet(consumers.keySet())
    }

    boolean contains(TaskProcessor process) {
        return consumers.containsKey(process)
    }

    Set<TaskProcessor> consumersOf(TaskProcessor process) {
        final Set<TaskProcessor> result = consumers.get(process)
        if( result == null )
            throw new IllegalArgumentException("Unknown process: ${process?.name ?: '<null>'}")
        return Collections.unmodifiableSet(result)
    }

    Set<TaskProcessor> producersOf(TaskProcessor process) {
        final Set<TaskProcessor> result = producers.get(process)
        if( result == null )
            throw new IllegalArgumentException("Unknown process: ${process?.name ?: '<null>'}")
        return Collections.unmodifiableSet(result)
    }

    private static Set<TaskProcessor> findImmediateConsumers(
        DAG.Vertex source,
        Map<DAG.Vertex, List<DAG.Vertex>> outgoing
    ) {
        final Set<TaskProcessor> result = newIdentityProcessSet()
        final Set<DAG.Vertex> visited = newIdentityVertexSet()
        final ArrayDeque<DAG.Vertex> queue = new ArrayDeque<>()

        final List<DAG.Vertex> first = outgoing.get(source)
        if( first != null )
            queue.addAll(first)

        while( !queue.isEmpty() ) {
            final DAG.Vertex current = queue.removeFirst()
            if( !visited.add(current) )
                continue

            final TaskProcessor process = current.process
            if( process != null ) {
                if( !process.is(source.process) )
                    result.add(process)
                continue
            }

            final List<DAG.Vertex> next = outgoing.get(current)
            if( next != null )
                queue.addAll(next)
        }

        return result
    }

    private static Set<TaskProcessor> newIdentityProcessSet() {
        return Collections.newSetFromMap(new IdentityHashMap<TaskProcessor, Boolean>())
    }

    private static Set<DAG.Vertex> newIdentityVertexSet() {
        return Collections.newSetFromMap(new IdentityHashMap<DAG.Vertex, Boolean>())
    }
}
