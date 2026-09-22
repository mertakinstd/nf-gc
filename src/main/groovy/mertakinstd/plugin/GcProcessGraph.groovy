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
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.dag.DAG
import nextflow.processor.TaskProcessor
import nextflow.script.ProcessConfigV1
import nextflow.script.ProcessConfigV2
import nextflow.script.params.EachInParam
import nextflow.script.params.FileInParam
import nextflow.script.params.InParam
import nextflow.script.params.InputsList
import nextflow.script.params.OutParam
import nextflow.script.params.OutputsList
import nextflow.script.params.TupleInParam
import nextflow.script.params.v2.ProcessOutputsDef
import nextflow.script.params.v2.ProcessTopic

/**
 * Process projection of Nextflow's workflow DAG plus producer output-port
 * provenance used by nf-gc.
 *
 * Both GC modes consume the same concrete output-port provenance. Process mode
 * closes that provenance at process boundaries; artifact mode additionally asks
 * whether an output port has a task-instance-safe runtime shape. Only terminal
 * ports and direct process routes are classified statically.
 * Any route that traverses a Nextflow operator remains FALLBACK here; generic
 * runtime processor closure, not operator-name interpretation, may prove a
 * narrower future-demand boundary for artifact mode.
 */
@CompileStatic
final class GcProcessGraph {

    static enum PortRouteKind {
        TERMINAL,
        DIRECT,
        FALLBACK
    }

    @CompileStatic
    private static final class RouteInfo {
        final Set<TaskProcessor> consumers = newIdentityProcessSet()
        final Set<DataflowProcessor> closureProcessors = newIdentityProcessorSet()
        boolean closureTrusted = true
    }

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
    private final Map<OutputPort, PortRouteKind> routeKinds
    private final Map<OutputPort, Set<DataflowProcessor>> runtimeClosureProcessors
    private final Set<OutputPort> globalSealPorts

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
            new IdentityHashMap<TaskProcessor, Set<OutputPort>>(),
            new LinkedHashMap<OutputPort, PortRouteKind>(),
            new LinkedHashMap<OutputPort, Set<DataflowProcessor>>(),
            new LinkedHashSet<OutputPort>()
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
        Map<TaskProcessor, Set<OutputPort>> inputPorts,
        Map<OutputPort, PortRouteKind> routeKinds,
        Map<OutputPort, Set<DataflowProcessor>> runtimeClosureProcessors,
        Set<OutputPort> globalSealPorts
    ) {
        this.consumers = consumers
        this.producers = producers
        this.outputPorts = outputPorts
        this.consumersByPort = consumersByPort
        this.inputPorts = inputPorts
        this.routeKinds = routeKinds
        this.runtimeClosureProcessors = runtimeClosureProcessors
        this.globalSealPorts = globalSealPorts
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
        final Map<OutputPort, PortRouteKind> routeKinds = new LinkedHashMap<>()
        final Map<OutputPort, Set<DataflowProcessor>> runtimeClosureProcessors = new LinkedHashMap<>()
        final Set<OutputPort> globalSealPorts = new LinkedHashSet<>()

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

            /* Nextflow output topics are connected outside the normal DAG edge
             * projection. A topic source may therefore look like a leaf here even
             * though channel.topic(...) can still consume it. Use Nextflow's own
             * output declaration metadata instead of treating that leaf as proof
             * of terminal demand. */
            final Set<Object> topicChannels = topicOutputChannels(process)

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
                final RouteInfo route = analyzeRoute(entry.value, outgoing, process)
                final Set<TaskProcessor> portConsumers = route.consumers
                final boolean requiresGlobalSeal = topicChannels == null || topicChannels.contains(entry.key)
                final PortRouteKind routeKind = requiresGlobalSeal
                    ? PortRouteKind.FALLBACK
                    : classifyRoute(entry.value, outgoing)
                outputPorts.get(process).add(port)
                if( requiresGlobalSeal )
                    globalSealPorts.add(port)
                consumersByPort.put(port, portConsumers)
                routeKinds.put(port, routeKind)
                runtimeClosureProcessors.put(
                    port,
                    routeKind == PortRouteKind.FALLBACK && route.closureTrusted
                        ? route.closureProcessors
                        : Collections.<DataflowProcessor>emptySet()
                )

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

        return new GcProcessGraph(
            consumers,
            producers,
            outputPorts,
            consumersByPort,
            inputPorts,
            routeKinds,
            runtimeClosureProcessors,
            globalSealPorts
        )
    }

    /**
     * Resolve Nextflow topic-backed output channels from the process declaration.
     * Topic routing is not represented as a normal producer edge in the DAG, so
     * a declared topic output must not be inferred terminal from a leaf edge.
     * A null result means declaration inspection failed and all ports for the
     * process should stay on conservative fallback.
     */
    private static Set<Object> topicOutputChannels(TaskProcessor process) {
        if( process == null )
            return Collections.emptySet()

        try {
            final Set<Object> result = Collections.newSetFromMap(new IdentityHashMap<Object,Boolean>())
            if( process.config instanceof ProcessConfigV1 ) {
                final OutputsList outputs = ((ProcessConfigV1)process.config).getOutputs()
                if( outputs == null || outputs.isEmpty() )
                    return result

                for( OutParam output : outputs ) {
                    if( output == null || !output.getChannelTopicName() )
                        continue
                    final Object channel = output.getOutChannel()
                    if( channel == null )
                        return null
                    result.add(channel)
                }
                return result
            }

            if( process.config instanceof ProcessConfigV2 ) {
                final ProcessOutputsDef outputs = ((ProcessConfigV2)process.config).getOutputs()
                if( outputs == null || outputs.topics == null || outputs.topics.isEmpty() )
                    return result

                for( ProcessTopic topic : outputs.topics ) {
                    if( topic == null || topic.channel == null )
                        return null
                    result.add(topic.channel)
                }
                return result
            }

            /* An unknown process-config model cannot prove the absence of topic
             * demand. Returning null forces every output port for that process onto
             * conservative FALLBACK rather than misclassifying a leaf as terminal. */
            return null
        }
        catch( Exception ignored ) {
            return null
        }
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

    PortRouteKind routeKind(OutputPort port) {
        final PortRouteKind result = routeKinds.get(port)
        return result != null ? result : PortRouteKind.FALLBACK
    }

    Set<DataflowProcessor> runtimeClosureProcessorsOf(OutputPort port) {
        final Set<DataflowProcessor> result = runtimeClosureProcessors.get(port)
        return result == null
            ? Collections.<DataflowProcessor>emptySet()
            : Collections.unmodifiableSet(result)
    }

    /**
     * True when Nextflow exposes legal demand for this output outside the normal
     * DAG/process projection (currently output topics), or when declaration
     * inspection could not prove that such demand is absent. No process/operator
     * closure is sufficient for these ports; only the successful global flow seal
     * proves that future demand is impossible.
     */
    boolean requiresGlobalSeal(OutputPort port) {
        return port != null && globalSealPorts.contains(port)
    }

    /** Return producer output ports backed by the exact channel object. */
    Set<OutputPort> outputPortsForChannel(Object channel) {
        if( channel == null )
            return Collections.emptySet()

        final Set<OutputPort> result = new LinkedHashSet<OutputPort>()
        for( Set<OutputPort> ports : outputPorts.values() ) {
            for( OutputPort port : ports ) {
                if( port.channel.is(channel) )
                    result.add(port)
            }
        }
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
        return routeKind(port) == PortRouteKind.TERMINAL
    }

    private static PortRouteKind classifyRoute(
        Collection<DAG.Edge> first,
        Map<DAG.Vertex, List<DAG.Edge>> outgoing
    ) {
        if( first == null || first.isEmpty() )
            return PortRouteKind.FALLBACK

        boolean sawTerminal = false
        boolean sawProcess = false
        for( DAG.Edge edge : first ) {
            final DAG.Vertex target = edge.to
            if( target == null || isLeaf(target, outgoing) ) {
                sawTerminal = true
                continue
            }
            if( target.process != null ) {
                sawProcess = true
                continue
            }
            return PortRouteKind.FALLBACK
        }

        if( sawProcess && !sawTerminal )
            return PortRouteKind.DIRECT
        if( sawTerminal && !sawProcess )
            return PortRouteKind.TERMINAL
        return PortRouteKind.FALLBACK
    }

    /**
     * Analyze a producer-port route without interpreting any operator type.
     * Runtime closure is trusted only when every traversed operator exposes a
     * DataflowProcessor and every process boundary can reserve the concrete file
     * input at TaskPending. Concrete reservations remain derived from TaskRun's
     * resolved inputs; this graph never interprets operator values.
     */
    private static RouteInfo analyzeRoute(
        Collection<DAG.Edge> first,
        Map<DAG.Vertex, List<DAG.Edge>> outgoing,
        TaskProcessor sourceProcess
    ) {
        final RouteInfo result = new RouteInfo()
        final Set<DAG.Vertex> visited = newIdentityVertexSet()
        final ArrayDeque<DAG.Edge> queue = new ArrayDeque<>()
        if( first != null )
            queue.addAll(first)

        while( !queue.isEmpty() ) {
            final DAG.Edge edge = queue.removeFirst()
            final DAG.Vertex target = edge.to
            if( target == null )
                continue

            final TaskProcessor process = target.process
            if( process != null ) {
                if( sourceProcess == null || !process.is(sourceProcess) )
                    result.consumers.add(process)
                final List<DataflowProcessor> operators = target.operators
                if( !reservationSafeBoundary(process, edge.channel) || operators == null || operators.isEmpty() )
                    result.closureTrusted = false
                else
                    result.closureProcessors.addAll(operators)
                continue
            }

            if( !visited.add(target) )
                continue

            final List<DAG.Edge> next = outgoing.get(target)
            final boolean leaf = next == null || next.isEmpty()
            final List<DataflowProcessor> operators = target.operators
            if( operators == null || operators.isEmpty() ) {
                if( !leaf )
                    result.closureTrusted = false
            }
            else {
                result.closureProcessors.addAll(operators)
            }
            if( next != null )
                queue.addAll(next)
        }

        if( result.closureProcessors.contains(null) )
            result.closureTrusted = false
        return result
    }

    private static boolean reservationSafeBoundary(TaskProcessor process, Object channel) {
        if( process == null || channel == null || !(process.config instanceof ProcessConfigV1) )
            return false
        try {
            /* Job arrays may defer TaskPending until the process operator stops;
             * `each` adds an extra task-expansion operator. Both stay on the
             * established fallback path. */
            if( process.config.getArray() > 0 )
                return false
            final InputsList inputs = ((ProcessConfigV1)process.config).getInputs()
            if( inputs == null || !inputs.ofType(EachInParam).isEmpty() )
                return false

            boolean matched = false
            for( InParam input : inputs ) {
                if( input == null || !channel.is(input.getRawChannel()) )
                    continue
                matched = true
                if( !containsFileInput(input) )
                    return false
            }
            return matched
        }
        catch( Exception ignored ) {
            return false
        }
    }

    private static boolean containsFileInput(InParam input) {
        if( input instanceof FileInParam )
            return true
        if( input instanceof TupleInParam ) {
            for( Object inner : ((TupleInParam)input).inner ) {
                if( inner instanceof FileInParam )
                    return true
            }
        }
        return false
    }

    private static boolean isLeaf(DAG.Vertex vertex, Map<DAG.Vertex,List<DAG.Edge>> outgoing) {
        if( vertex == null || vertex.process != null )
            return false
        final List<DataflowProcessor> operators = vertex.operators
        if( operators != null && !operators.isEmpty() )
            return false
        final List<DAG.Edge> next = outgoing.get(vertex)
        return next == null || next.isEmpty()
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

    private static Set<DataflowProcessor> newIdentityProcessorSet() {
        return Collections.newSetFromMap(new IdentityHashMap<DataflowProcessor, Boolean>())
    }

    private static Set<DAG.Vertex> newIdentityVertexSet() {
        return Collections.newSetFromMap(new IdentityHashMap<DAG.Vertex, Boolean>())
    }
}
