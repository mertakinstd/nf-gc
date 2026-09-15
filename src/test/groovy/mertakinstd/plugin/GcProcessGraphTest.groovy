package mertakinstd.plugin

import nextflow.dag.DAG
import nextflow.processor.TaskProcessor
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
        return Mock(TaskProcessor) {
            getName() >> name
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
