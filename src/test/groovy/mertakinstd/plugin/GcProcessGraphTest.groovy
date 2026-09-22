package mertakinstd.plugin

import groovyx.gpars.dataflow.DataflowBroadcast
import groovyx.gpars.dataflow.operator.DataflowProcessor
import nextflow.dag.DAG
import nextflow.processor.TaskProcessor
import nextflow.script.ProcessConfigV1
import nextflow.script.ProcessConfigV2
import nextflow.script.params.OutParam
import nextflow.script.params.OutputsList
import nextflow.script.params.v2.ProcessOutputsDef
import spock.lang.Specification

class GcProcessGraphTest extends Specification {

    def 'keeps sibling output channels on independent consumer sets'() {
        given:
        def producer = process('SOURCE')
        def fast = process('FAST')
        def slow = process('SLOW')
        def sourceVertex = vertex(producer)
        def fastVertex = vertex(fast)
        def slowVertex = vertex(slow)
        def fastChannel = new Object()
        def slowChannel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex, fastVertex, slowVertex],
            [
                edge(sourceVertex, fastVertex, fastChannel),
                edge(sourceVertex, slowVertex, slowChannel)
            ]
        ))

        expect:
        graph.consumersOf(graph.outputPort(producer, fastChannel)) == ([fast] as Set)
        graph.consumersOf(graph.outputPort(producer, slowChannel)) == ([slow] as Set)
    }

    def 'keeps every consumer when one output channel fans out'() {
        given:
        def producer = process('SOURCE')
        def fast = process('FAST')
        def slow = process('SLOW')
        def sourceVertex = vertex(producer)
        def fastVertex = vertex(fast)
        def slowVertex = vertex(slow)
        def sharedChannel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex, fastVertex, slowVertex],
            [
                edge(sourceVertex, fastVertex, sharedChannel),
                edge(sourceVertex, slowVertex, sharedChannel)
            ]
        ))

        expect:
        graph.consumersOf(graph.outputPort(producer, sharedChannel)) == ([fast, slow] as Set)
    }

    def 'follows an output port through non-process operators to the first consumer process'() {
        given:
        def producer = process('SOURCE')
        def consumer = process('CONSUMER')
        def sourceVertex = vertex(producer)
        def operatorVertex = vertex(null)
        def consumerVertex = vertex(consumer)
        def sourceChannel = new Object()
        def mappedChannel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex, operatorVertex, consumerVertex],
            [
                edge(sourceVertex, operatorVertex, sourceChannel),
                edge(operatorVertex, consumerVertex, mappedChannel)
            ]
        ))

        expect:
        graph.consumersOf(graph.outputPort(producer, sourceChannel)) == ([consumer] as Set)
        graph.inputPortsOf(consumer) == ([graph.outputPort(producer, sourceChannel)] as Set)
    }

    def 'does not treat an operator-backed output with no process consumer as terminal'() {
        given:
        def producer = process('SOURCE')
        def processor = Mock(DataflowProcessor)
        def sourceVertex = vertex(producer)
        def operatorVertex = Mock(DAG.Vertex) {
            getProcess() >> null
            getOperators() >> [processor]
        }
        def sourceChannel = new Object()
        def mappedChannel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex, operatorVertex],
            [
                edge(sourceVertex, operatorVertex, sourceChannel),
                edge(operatorVertex, null, mappedChannel)
            ]
        ))
        def port = graph.outputPort(producer, sourceChannel)

        expect:
        graph.routeKind(port) == GcProcessGraph.PortRouteKind.FALLBACK
        graph.consumersOf(port).empty
        graph.runtimeClosureProcessorsOf(port) == ([processor] as Set)
    }

    def 'does not treat a declared topic output as terminal when the DAG edge is a leaf'() {
        given:
        def channel = new DataflowBroadcast()
        def output = Mock(OutParam) {
            getOutChannel() >> channel
            getChannelTopicName() >> 'versions'
        }
        def outputs = new OutputsList()
        outputs.add(output)
        def config = Mock(ProcessConfigV1) {
            getOutputs() >> outputs
        }
        def producer = Mock(TaskProcessor) {
            getName() >> 'SOURCE'
            getConfig() >> config
        }
        def sourceVertex = vertex(producer)
        def graph = GcProcessGraph.from(dag(
            [sourceVertex],
            [edge(sourceVertex, null, channel)]
        ))
        def port = graph.outputPort(producer, channel)

        expect:
        port != null
        graph.routeKind(port) == GcProcessGraph.PortRouteKind.FALLBACK
        !graph.isTerminal(port)
        graph.consumersOf(port).empty
        graph.runtimeClosureProcessorsOf(port).empty
        graph.requiresGlobalSeal(port)
    }

    def 'requires the global flow seal when a topic-backed port also has a normal process consumer'() {
        given:
        def channel = new DataflowBroadcast()
        def output = Mock(OutParam) {
            getOutChannel() >> channel
            getChannelTopicName() >> 'versions'
        }
        def outputs = new OutputsList()
        outputs.add(output)
        def config = Mock(ProcessConfigV1) {
            getOutputs() >> outputs
        }
        def producer = Mock(TaskProcessor) {
            getName() >> 'SOURCE'
            getConfig() >> config
        }
        def consumer = process('SINK')
        def sourceVertex = vertex(producer)
        def sinkVertex = vertex(consumer)
        def graph = GcProcessGraph.from(dag(
            [sourceVertex, sinkVertex],
            [edge(sourceVertex, sinkVertex, channel)]
        ))
        def port = graph.outputPort(producer, channel)

        expect:
        port != null
        graph.routeKind(port) == GcProcessGraph.PortRouteKind.FALLBACK
        graph.consumersOf(port) == ([consumer] as Set)
        graph.requiresGlobalSeal(port)
        !graph.isTerminal(port)
    }

    def 'does not treat a typed V2 topic output as terminal when the DAG edge is a leaf'() {
        given:
        def channel = new DataflowBroadcast()
        def outputs = new ProcessOutputsDef()
        outputs.addTopic('payload', 'versions')
        outputs.topics.first().channel = channel
        def config = Mock(ProcessConfigV2) {
            getOutputs() >> outputs
        }
        def producer = Mock(TaskProcessor) {
            getName() >> 'SOURCE'
            getConfig() >> config
        }
        def sourceVertex = vertex(producer)
        def graph = GcProcessGraph.from(dag(
            [sourceVertex],
            [edge(sourceVertex, null, channel)]
        ))
        def port = graph.outputPort(producer, channel)

        expect:
        port != null
        graph.routeKind(port) == GcProcessGraph.PortRouteKind.FALLBACK
        !graph.isTerminal(port)
        graph.consumersOf(port).empty
        graph.requiresGlobalSeal(port)
    }

    def 'falls back globally when typed topic metadata exists before its channel is available'() {
        given:
        def channel = new DataflowBroadcast()
        def outputs = new ProcessOutputsDef()
        outputs.addTopic('payload', 'versions')
        def config = Mock(ProcessConfigV2) {
            getOutputs() >> outputs
        }
        def producer = Mock(TaskProcessor) {
            getName() >> 'SOURCE'
            getConfig() >> config
        }
        def sourceVertex = vertex(producer)
        def graph = GcProcessGraph.from(dag(
            [sourceVertex],
            [edge(sourceVertex, null, channel)]
        ))
        def port = graph.outputPort(producer, channel)

        expect:
        port != null
        graph.routeKind(port) == GcProcessGraph.PortRouteKind.FALLBACK
        !graph.isTerminal(port)
        graph.requiresGlobalSeal(port)
    }

    def 'uses the global conservative fallback when process config cannot prove topic absence'() {
        given:
        def producer = Mock(TaskProcessor) {
            getName() >> 'SOURCE'
            getConfig() >> null
        }
        def sourceVertex = vertex(producer)
        def channel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex],
            [edge(sourceVertex, null, channel)]
        ))
        def port = graph.outputPort(producer, channel)

        expect:
        port != null
        graph.routeKind(port) == GcProcessGraph.PortRouteKind.FALLBACK
        !graph.isTerminal(port)
        graph.consumersOf(port).empty
        graph.requiresGlobalSeal(port)
    }

    def 'marks an unconsumed producer output port as terminal'() {
        given:
        def producer = process('SOURCE')
        def sourceVertex = vertex(producer)
        def channel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex],
            [edge(sourceVertex, null, channel)]
        ))
        def port = graph.outputPort(producer, channel)

        expect:
        port != null
        graph.isTerminal(port)
        graph.consumersOf(port).empty
    }

    private TaskProcessor process(String name) {
        def outputs = new OutputsList()
        def config = Mock(ProcessConfigV1) {
            getOutputs() >> outputs
        }
        return Mock(TaskProcessor) {
            getName() >> name
            getConfig() >> config
        }
    }

    private DAG.Vertex vertex(TaskProcessor process) {
        return Mock(DAG.Vertex) {
            getProcess() >> process
        }
    }

    private DAG.Edge edge(DAG.Vertex from, DAG.Vertex to, Object channel) {
        return Mock(DAG.Edge) {
            getFrom() >> from
            getTo() >> to
            getChannel() >> channel
        }
    }

    private DAG dag(List<DAG.Vertex> vertices, List<DAG.Edge> edges) {
        return Mock(DAG) {
            getVertices() >> vertices
            getEdges() >> edges
        }
    }
}
