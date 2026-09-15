package mertakinstd.plugin

import nextflow.dag.DAG
import nextflow.processor.TaskProcessor
import spock.lang.Specification

class GcOutputDependencyStateTest extends Specification {

    def 'closes sibling output ports independently after their own consumers terminate'() {
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
        def processState = new GcDependencyState(graph)
        def outputState = new GcOutputDependencyState(graph, processState)
        def fastPort = graph.outputPort(producer, fastChannel)
        def slowPort = graph.outputPort(producer, slowChannel)

        when:
        processState.onProcessTerminate(producer)
        def afterProducer = outputState.onProcessTerminate(producer)
        processState.onProcessTerminate(fast)
        def afterFast = outputState.onProcessTerminate(fast)

        then:
        afterProducer.empty
        afterFast == [fastPort]
        outputState.isClosed(fastPort)
        !outputState.isClosed(slowPort)

        when:
        processState.onProcessTerminate(slow)
        def afterSlow = outputState.onProcessTerminate(slow)

        then:
        afterSlow == [slowPort]
        outputState.isClosed(slowPort)
    }

    def 'does not close a fan-out port until every consumer process terminates'() {
        given:
        def producer = process('SOURCE')
        def fast = process('FAST')
        def slow = process('SLOW')
        def sourceVertex = vertex(producer)
        def fastVertex = vertex(fast)
        def slowVertex = vertex(slow)
        def channel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex, fastVertex, slowVertex],
            [
                edge(sourceVertex, fastVertex, channel),
                edge(sourceVertex, slowVertex, channel)
            ]
        ))
        def processState = new GcDependencyState(graph)
        def outputState = new GcOutputDependencyState(graph, processState)
        def port = graph.outputPort(producer, channel)

        when:
        processState.onProcessTerminate(producer)
        outputState.onProcessTerminate(producer)
        processState.onProcessTerminate(fast)
        def afterFast = outputState.onProcessTerminate(fast)

        then:
        afterFast.empty
        !outputState.isClosed(port)

        when:
        processState.onProcessTerminate(slow)
        def afterSlow = outputState.onProcessTerminate(slow)

        then:
        afterSlow == [port]
        outputState.isClosed(port)
    }

    def 'does not close an output port before its producer process terminates'() {
        given:
        def producer = process('SOURCE')
        def consumer = process('CONSUMER')
        def sourceVertex = vertex(producer)
        def consumerVertex = vertex(consumer)
        def channel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex, consumerVertex],
            [edge(sourceVertex, consumerVertex, channel)]
        ))
        def processState = new GcDependencyState(graph)
        def outputState = new GcOutputDependencyState(graph, processState)
        def port = graph.outputPort(producer, channel)

        when:
        processState.onProcessTerminate(consumer)
        def afterConsumer = outputState.onProcessTerminate(consumer)

        then:
        afterConsumer.empty
        !outputState.isClosed(port)

        when:
        processState.onProcessTerminate(producer)
        def afterProducer = outputState.onProcessTerminate(producer)

        then:
        afterProducer == [port]
        outputState.isClosed(port)
    }

    def 'never dependency-closes a terminal output port'() {
        given:
        def producer = process('SOURCE')
        def sourceVertex = vertex(producer)
        def channel = new Object()
        def graph = GcProcessGraph.from(dag(
            [sourceVertex],
            [edge(sourceVertex, null, channel)]
        ))
        def processState = new GcDependencyState(graph)
        def outputState = new GcOutputDependencyState(graph, processState)
        def port = graph.outputPort(producer, channel)

        when:
        processState.onProcessTerminate(producer)
        def closed = outputState.onProcessTerminate(producer)

        then:
        closed.empty
        !outputState.isClosed(port)
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
