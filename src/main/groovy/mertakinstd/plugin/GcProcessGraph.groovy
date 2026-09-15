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
import java.util.Collection
import java.util.Collections
import java.util.IdentityHashMap
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import nextflow.dag.DAG
import nextflow.processor.TaskProcessor

/**
 * Process projection of Nextflow's workflow DAG plus producer output-port
 * provenance used by artifact-mode GC.
 *
 * Process dependencies collapse paths through non-process vertices until the
 * first downstream process is reached. Output ports preserve the channel that
 * leaves a producer process and apply the same traversal from that channel, so
 * sibling outputs retain independent consumer sets without introducing
 * task-instance scheduling semantics.
 */
@CompileStatic
final class GcProcessGraph {

    /**
     * A producer output port identified by the exact TaskProcessor and channel
     * objects represented in Nextflow's DAG.
     */
    @CompileStatic
    static final class OutputPort {
        final TaskProcessor producer
        final Object channel

        OutputPort(TaskProcessor producer, Object channel) {
            if( producer == null )
                throw new IllegalArgumentException('Output port producer must not be null')
            if( channel == null )
                throw new IllegalArgumentException('Output port channel must not be null')
            this.producer = producer
            this.channel = channel
        }

        @Override
        boolean equals(Object other) {
            if( this.is(other) )
                return true
            if( !(other instanceof OutputPort) )
                return false
            final OutputPort that = (OutputPort)other
            return producer.is(that.producer) && channel.is(that.channel)
        }

        @Override
        int hashCode() {
            return 31 * System.identityHashCode(producer) + System.identityHashCode(channel)
        }

        @Override
        String toString() {
            return "${producer.name}@${Integer.toHexString(System.identityHashCode(channel))}".toString()
        }
    }

    private final Map<TaskProcessor, Set<TaskProcessor>> consumers
    private final Map<TaskProcessor, Set<TaskProcessor>> producers
    private final Map<TaskProcessor, Set<OutputPort>> outputPorts
    private final Map<OutputPort, Set<TaskProcessor>> consumersByPort
    private final Map<TaskProcessor, Set<OutputPort>> inputPorts

    @PackageScope
    GcProcessGraph(
        Map<TaskProcessor, Set<TaskProcessor>> consumers,
        Map<TaskProcessor, Set<TaskProcessor>> producers
    ) {
        this(
            consumers,
            producers,
            new IdentityHashMap<TaskProcessor, Set<OutputPort>>(),
            new LinkedHashMap<OutputPort, Set<TaskProcessor>>(),
            new IdentityHashMap<TaskProcessor, Set<OutputPort>>()
        )
        for( TaskProcessor process : consumers.keySet() ) {
            this.outputPorts.put(process, new LinkedHashSet<OutputPort>())
            this.inputPorts.put(process, new LinkedHashSet<OutputPort>())
        }
    }

    @PackageScope
    GcProcessGraph(
        Map<TaskProcessor, Set<TaskProcessor>> consumers,
        Map<TaskProcessor, Set<TaskProcessor>> producers,
        Map<TaskProcessor, Set<OutputPort>> outputPorts,
        Map<OutputPort, Set<TaskProcessor>> consumersByPort,
        Map<TaskProcessor, Set<OutputPort>> inputPorts
    ) {
        this.consumers = consumers
        this.producers = producers
        this.outputPorts = outputPorts
        this.consumersByPort = consumersByPort
        this.inputPorts = inputPorts
    }

    static GcProcessGraph from(DAG dag) {
        if( dag == null )
            throw new IllegalArgumentException('Nextflow DAG must not be null')

        final Map<DAG.Vertex, List<DAG.Edge>> outgoing = new IdentityHashMap<>()

        for( DAG.Vertex vertex : dag.vertices )
            outgoing.put(vertex, new ArrayList<DAG.Edge>())

        for( DAG.Edge edge : dag.edges ) {
            final DAG.Vertex from = edge.from
            if( from == null )
                continue

            List<DAG.Edge> targets = outgoing.get(from)
            if( targets == null ) {
                targets = new ArrayList<DAG.Edge>()
                outgoing.put(from, targets)
            }
            targets.add(edge)
        }

        final Map<TaskProcessor, Set<TaskProcessor>> consumers = new IdentityHashMap<>()
        final Map<TaskProcessor, Set<TaskProcessor>> producers = new IdentityHashMap<>()
        final Map<TaskProcessor, Set<OutputPort>> outputPorts = new IdentityHashMap<>()
        final Map<OutputPort, Set<TaskProcessor>> consumersByPort = new LinkedHashMap<>()
        final Map<TaskProcessor, Set<OutputPort>> inputPorts = new IdentityHashMap<>()

        for( DAG.Vertex vertex : dag.vertices ) {
            final TaskProcessor process = vertex.process
            if( process == null )
                continue

            consumers.put(process, findImmediateConsumers(vertex, outgoing))
            producers.put(process, newIdentityProcessSet())
            outputPorts.put(process, new LinkedHashSet<OutputPort>())
            inputPorts.put(process, new LinkedHashSet<OutputPort>())
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

        for( DAG.Vertex vertex : dag.vertices ) {
            final TaskProcessor process = vertex.process
            if( process == null )
                continue

            final List<DAG.Edge> edges = outgoing.get(vertex)
            if( edges == null || edges.isEmpty() )
                continue

            final Map<Object, List<DAG.Edge>> byChannel = new IdentityHashMap<>()
            for( DAG.Edge edge : edges ) {
                if( edge.channel == null )
                    continue
                List<DAG.Edge> sameChannel = byChannel.get(edge.channel)
                if( sameChannel == null ) {
                    sameChannel = new ArrayList<DAG.Edge>()
                    byChannel.put(edge.channel, sameChannel)
                }
                sameChannel.add(edge)
            }

            for( Map.Entry<Object, List<DAG.Edge>> entry : byChannel.entrySet() ) {
                final OutputPort port = new OutputPort(process, entry.key)
                final Set<TaskProcessor> portConsumers = findImmediateConsumers(entry.value, outgoing, process)
                outputPorts.get(process).add(port)
                consumersByPort.put(port, portConsumers)

                for( TaskProcessor consumer : portConsumers ) {
                    final Set<OutputPort> upstreamPorts = inputPorts.get(consumer)
                    if( upstreamPorts == null )
                        throw new IllegalStateException(
                            "DAG output-port consumer is missing a process vertex: ${consumer.name}"
                        )
                    upstreamPorts.add(port)
                }
            }
        }

        return new GcProcessGraph(consumers, producers, outputPorts, consumersByPort, inputPorts)
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

    Set<OutputPort> outputPortsOf(TaskProcessor process) {
        final Set<OutputPort> result = outputPorts.get(process)
        if( result == null )
            throw new IllegalArgumentException("Unknown process: ${process?.name ?: '<null>'}")
        return Collections.unmodifiableSet(result)
    }

    Set<OutputPort> inputPortsOf(TaskProcessor process) {
        final Set<OutputPort> result = inputPorts.get(process)
        if( result == null )
            throw new IllegalArgumentException("Unknown process: ${process?.name ?: '<null>'}")
        return Collections.unmodifiableSet(result)
    }

    Set<TaskProcessor> consumersOf(OutputPort port) {
        final Set<TaskProcessor> result = consumersByPort.get(port)
        if( result == null )
            throw new IllegalArgumentException("Unknown output port: ${port}")
        return Collections.unmodifiableSet(result)
    }

    OutputPort outputPort(TaskProcessor process, Object channel) {
        if( process == null || channel == null )
            return null
        final Set<OutputPort> ports = outputPorts.get(process)
        if( ports == null )
            return null
        for( OutputPort port : ports ) {
            if( port.channel.is(channel) )
                return port
        }
        return null
    }

    boolean isTerminal(OutputPort port) {
        return consumersOf(port).isEmpty()
    }

    private static Set<TaskProcessor> findImmediateConsumers(
        DAG.Vertex source,
        Map<DAG.Vertex, List<DAG.Edge>> outgoing
    ) {
        final List<DAG.Edge> first = outgoing.get(source)
        return first == null
            ? newIdentityProcessSet()
            : findImmediateConsumers(first, outgoing, source.process)
    }

    private static Set<TaskProcessor> findImmediateConsumers(
        Collection<DAG.Edge> first,
        Map<DAG.Vertex, List<DAG.Edge>> outgoing,
        TaskProcessor sourceProcess
    ) {
        final Set<TaskProcessor> result = newIdentityProcessSet()
        final Set<DAG.Vertex> visited = newIdentityVertexSet()
        final ArrayDeque<DAG.Vertex> queue = new ArrayDeque<>()

        for( DAG.Edge edge : first ) {
            if( edge.to != null )
                queue.add(edge.to)
        }

        while( !queue.isEmpty() ) {
            final DAG.Vertex current = queue.removeFirst()
            if( !visited.add(current) )
                continue

            final TaskProcessor process = current.process
            if( process != null ) {
                if( sourceProcess == null || !process.is(sourceProcess) )
                    result.add(process)
                continue
            }

            final List<DAG.Edge> next = outgoing.get(current)
            if( next == null )
                continue
            for( DAG.Edge edge : next ) {
                if( edge.to != null )
                    queue.add(edge.to)
            }
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
