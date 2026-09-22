package mertakinstd.plugin

import java.nio.file.Files
import java.nio.file.Path
import java.util.function.BiConsumer

import groovyx.gpars.dataflow.DataflowBroadcast
import nextflow.Session
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfigV1
import nextflow.script.params.FileOutParam
import nextflow.script.params.OutputsList
import spock.lang.Specification
import spock.lang.TempDir

class GcArtifactRegistryTest extends Specification {

    @TempDir
    Path tempDir

    def 'deletes an artifact when it was registered before dependency closure'() {
        given:
        def producer = process('PRODUCER')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def registry = new GcArtifactRegistry(graph)
        def workDir = Files.createDirectories(tempDir.resolve('producer'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)

        when:
        def update = registry.onTaskComplete(task)
        def deletions = registry.onDependencyClosed(producer)

        then:
        update.tracked == [artifact]
        update.deletions.empty
        deletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(artifact)
    }

    def 'deletes an artifact when dependency closure arrives before task completion'() {
        given:
        def producer = process('PRODUCER')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def registry = new GcArtifactRegistry(graph)
        def workDir = Files.createDirectories(tempDir.resolve('producer'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)

        when:
        def before = registry.onDependencyClosed(producer)
        def update = registry.onTaskComplete(task)

        then:
        before.empty
        update.tracked == [artifact]
        update.deletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(artifact)
    }

    def 'duplicate completion and closure callbacks are idempotent'() {
        given:
        def producer = process('PRODUCER')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def registry = new GcArtifactRegistry(graph)
        def workDir = Files.createDirectories(tempDir.resolve('producer'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)

        when:
        def firstUpdate = registry.onTaskComplete(task)
        def duplicateUpdate = registry.onTaskComplete(task)
        def firstClose = registry.onDependencyClosed(producer)
        def duplicateClose = registry.onDependencyClosed(producer)
        def afterDeleteUpdate = registry.onTaskComplete(task)

        then:
        firstUpdate.tracked == [artifact]
        duplicateUpdate.tracked.empty
        duplicateUpdate.deletions.empty
        firstClose*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        duplicateClose.empty
        afterDeleteUpdate.tracked.empty
        afterDeleteUpdate.deletions.empty
        !Files.exists(artifact)
    }

    def 'pass-through staged output holds the upstream artifact until relay closure'() {
        given:
        def producer = process('PRODUCER')
        def relay = process('RELAY')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, relay], [relay, consumer]])
        def registry = new GcArtifactRegistry(graph)

        def producerWork = Files.createDirectories(tempDir.resolve('producer'))
        def source = Files.writeString(producerWork.resolve('source.txt'), 'source')
        def producerTask = successfulTask(producer, producerWork, [source] as Set<Path>)

        def relayWork = Files.createDirectories(tempDir.resolve('relay'))
        def staged = relayWork.resolve('source.txt')
        Files.createSymbolicLink(staged, source)
        def relayTask = successfulTask(
            relay,
            relayWork,
            [staged] as Set<Path>,
            ['source.txt': source]
        )

        when:
        registry.onTaskStart(relayTask)
        def producerClosed = registry.onDependencyClosed(producer)
        def producerUpdate = registry.onTaskComplete(producerTask)

        then:
        producerClosed.empty
        producerUpdate.tracked == [source]
        producerUpdate.deletions.empty
        Files.exists(source)

        when:
        def relayUpdate = registry.onTaskComplete(relayTask)

        then:
        relayUpdate.tracked.empty
        relayUpdate.held == [source]
        relayUpdate.deletions.empty
        Files.exists(source)

        when:
        def relayClosed = registry.onDependencyClosed(relay)

        then:
        relayClosed*.process == [producer]
        relayClosed*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(source)
    }


    def 'provisional input hold prevents callback-order deletion for a normal consumer'() {
        given:
        def producer = process('PRODUCER')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def registry = new GcArtifactRegistry(graph)

        def producerWork = Files.createDirectories(tempDir.resolve('producer-normal'))
        def source = Files.writeString(producerWork.resolve('source.txt'), 'source')
        def producerTask = successfulTask(producer, producerWork, [source] as Set<Path>)

        def consumerWork = Files.createDirectories(tempDir.resolve('consumer-normal'))
        def staged = consumerWork.resolve('source.txt')
        Files.createSymbolicLink(staged, source)
        def consumed = Files.writeString(consumerWork.resolve('consumed.txt'), 'consumed')
        def consumerTask = successfulTask(
            consumer,
            consumerWork,
            [consumed] as Set<Path>,
            ['source.txt': source]
        )

        when:
        registry.onTaskComplete(producerTask)
        registry.onTaskStart(consumerTask)
        def producerClosed = registry.onDependencyClosed(producer)

        then:
        producerClosed.empty
        Files.exists(source)

        when:
        def consumerUpdate = registry.onTaskComplete(consumerTask)

        then:
        consumerUpdate.deletions*.process == [producer]
        consumerUpdate.deletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(source)
    }


    def 'process mode reclaims sibling Paths at independent output-port process closures'() {
        given:
        /* FileOutParam.getOutChannel() is a DataflowWriteChannel in
         * Nextflow 26.04.6; use the real channel shape so this fixture exercises
         * Path-to-port resolution instead of Spock's incompatible-return fallback. */
        def fastChannel = new DataflowBroadcast()
        def slowChannel = new DataflowBroadcast()
        def fastParam = Mock(FileOutParam) { getOutChannel() >> fastChannel }
        def slowParam = Mock(FileOutParam) { getOutChannel() >> slowChannel }
        def outputs = new OutputsList()
        outputs.add(fastParam)
        outputs.add(slowParam)
        def producerConfig = Mock(ProcessConfigV1) { getOutputs() >> outputs }
        def producer = Mock(TaskProcessor) {
            getName() >> 'SOURCE'
            getConfig() >> producerConfig
        }
        def fastConsumer = process('FAST')
        def slowConsumer = process('SLOW')

        def consumers = new IdentityHashMap<TaskProcessor,Set<TaskProcessor>>()
        def producers = new IdentityHashMap<TaskProcessor,Set<TaskProcessor>>()
        [producer, fastConsumer, slowConsumer].each { process ->
            consumers.put(process, identityProcessSet())
            producers.put(process, identityProcessSet())
        }
        consumers.get(producer).addAll([fastConsumer, slowConsumer])
        producers.get(fastConsumer).add(producer)
        producers.get(slowConsumer).add(producer)

        def fastPort = new GcProcessGraph.OutputPort(producer, fastChannel)
        def slowPort = new GcProcessGraph.OutputPort(producer, slowChannel)
        def outputPorts = new IdentityHashMap<TaskProcessor,Set<GcProcessGraph.OutputPort>>()
        outputPorts.put(producer, new LinkedHashSet<GcProcessGraph.OutputPort>([fastPort, slowPort]))
        outputPorts.put(fastConsumer, new LinkedHashSet<GcProcessGraph.OutputPort>())
        outputPorts.put(slowConsumer, new LinkedHashSet<GcProcessGraph.OutputPort>())
        def consumersByPort = new LinkedHashMap<GcProcessGraph.OutputPort,Set<TaskProcessor>>()
        consumersByPort.put(fastPort, identityProcessSet([fastConsumer]))
        consumersByPort.put(slowPort, identityProcessSet([slowConsumer]))
        def inputPorts = new IdentityHashMap<TaskProcessor,Set<GcProcessGraph.OutputPort>>()
        inputPorts.put(producer, new LinkedHashSet<GcProcessGraph.OutputPort>())
        inputPorts.put(fastConsumer, new LinkedHashSet<GcProcessGraph.OutputPort>([fastPort]))
        inputPorts.put(slowConsumer, new LinkedHashSet<GcProcessGraph.OutputPort>([slowPort]))
        def routeKinds = new LinkedHashMap<GcProcessGraph.OutputPort,GcProcessGraph.PortRouteKind>()
        routeKinds.put(fastPort, GcProcessGraph.PortRouteKind.DIRECT)
        routeKinds.put(slowPort, GcProcessGraph.PortRouteKind.DIRECT)
        def runtimeClosure = new LinkedHashMap<GcProcessGraph.OutputPort,Set<groovyx.gpars.dataflow.operator.DataflowProcessor>>()
        runtimeClosure.put(fastPort, Collections.emptySet())
        runtimeClosure.put(slowPort, Collections.emptySet())
        def graph = new GcProcessGraph(
            consumers, producers, outputPorts, consumersByPort, inputPorts,
            routeKinds, runtimeClosure, new LinkedHashSet<GcProcessGraph.OutputPort>()
        )
        def trace = []
        def traceSink = { String event, String detail ->
            trace.add("${event}\t${detail}".toString())
        } as BiConsumer<String,String>
        def registry = new GcArtifactRegistry(graph, null, GcMode.PROCESS, traceSink)

        def workDir = Files.createDirectories(tempDir.resolve('process-sibling-ports'))
        def fastArtifact = Files.writeString(workDir.resolve('fast.txt'), 'fast')
        def slowArtifact = Files.writeString(workDir.resolve('slow.txt'), 'slow')
        def taskConfig = Mock(TaskConfig) { getPublishDir() >> Collections.emptyList() }
        def task = Mock(TaskRun) {
            getProcessor() >> producer
            getCached() >> false
            isSuccess() >> true
            getWorkDir() >> workDir
            getTargetDir() >> workDir
            hasTypedInputsOutputs() >> false
            getOutputsByType(FileOutParam) >> [(fastParam): fastArtifact, (slowParam): slowArtifact]
            getInputFilesMap() >> Collections.emptyMap()
            getInputs() >> Collections.emptyMap()
            getConfig() >> taskConfig
            getName() >> producer.name
        }

        when:
        def update = registry.onTaskComplete(task)
        registry.onProcessTerminated(producer)
        registry.onProcessTerminated(fastConsumer)
        def fastDeleted = registry.onOutputClosed(fastPort)

        then:
        update.tracked as Set == ([fastArtifact, slowArtifact] as Set)
        trace.any { it.startsWith("ARTIFACT_ROUTE\t${fastArtifact}\t${fastPort}\tDIRECT\t") && it.contains('\tFAST\tfalse') }
        trace.any { it.startsWith("ARTIFACT_ROUTE\t${slowArtifact}\t${slowPort}\tDIRECT\t") && it.contains('\tSLOW\tfalse') }
        trace.any { it.startsWith("OUTPUT_PORT_CLOSED\t${fastPort}\tDIRECT\tFAST\tfalse") }
        fastDeleted*.path == [fastArtifact]
        !Files.exists(fastArtifact)
        Files.exists(slowArtifact)

        when:
        registry.onProcessTerminated(slowConsumer)
        def slowDeleted = registry.onOutputClosed(slowPort)

        then:
        trace.any { it.startsWith("OUTPUT_PORT_CLOSED\t${slowPort}\tDIRECT\tSLOW\tfalse") }
        slowDeleted*.path == [slowArtifact]
        !Files.exists(slowArtifact)
    }


    def 'successful flow completion is a final coarse seal even if process closure was not observed'() {
        given:
        def producer = process('PRODUCER')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def session = Mock(Session) {
            getOutputs() >> Collections.emptyMap()
            isSuccess() >> true
        }
        def registry = new GcArtifactRegistry(graph, session, GcMode.PROCESS)
        def workDir = Files.createDirectories(tempDir.resolve('flow-seal'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)

        when:
        def update = registry.onTaskComplete(task)
        def deletions = registry.onFlowComplete()

        then:
        update.tracked == [artifact]
        deletions*.path == [artifact]
        deletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(artifact)
    }


    def 'process mode waits for successful flow completion when demand requires the global seal'() {
        given:
        def producer = process('PRODUCER')
        def session = Mock(Session) {
            getOutputs() >> Collections.emptyMap()
            isSuccess() >> true
        }
        def registry = new GcArtifactRegistry(globalSealGraph(producer), session, GcMode.PROCESS)
        def workDir = Files.createDirectories(tempDir.resolve('process-global-seal'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)

        when:
        def update = registry.onTaskComplete(task)
        def processClosed = registry.onDependencyClosed(producer)

        then:
        update.tracked == [artifact]
        processClosed.empty
        Files.exists(artifact)

        when:
        def flowDeletions = registry.onFlowComplete()

        then:
        flowDeletions*.path == [artifact]
        flowDeletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(artifact)
    }


    def 'artifact process fallback also respects the global flow seal'() {
        given:
        def producer = process('PRODUCER')
        def session = Mock(Session) {
            getOutputs() >> Collections.emptyMap()
            isSuccess() >> true
        }
        def registry = new GcArtifactRegistry(globalSealGraph(producer), session, GcMode.ARTIFACT)
        def workDir = Files.createDirectories(tempDir.resolve('artifact-global-seal'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)

        when:
        def update = registry.onTaskComplete(task)
        def processClosed = registry.onDependencyClosed(producer)

        then:
        update.tracked == [artifact]
        processClosed.empty
        Files.exists(artifact)

        when:
        def flowDeletions = registry.onFlowComplete()

        then:
        flowDeletions*.path == [artifact]
        flowDeletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(artifact)
    }


    def 'artifact mode falls back to process closure when exact output-port provenance is unavailable'() {
        given:
        def producer = process('PRODUCER')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def registry = new GcArtifactRegistry(graph, null, GcMode.ARTIFACT)
        def workDir = Files.createDirectories(tempDir.resolve('typed-artifact'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)

        when:
        def update = registry.onTaskComplete(task)
        def deletions = registry.onDependencyClosed(producer)

        then:
        update.tracked == [artifact]
        update.deletions.empty
        deletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(artifact)
    }

    def 'artifact output-port resolver failure falls back to process liveness when retention does not depend on it'() {
        given:
        def legacyConfig = Mock(ProcessConfigV1) {
            getOutputs() >> { throw new IllegalStateException('broken output provenance') }
        }
        def producer = Mock(TaskProcessor) {
            getName() >> 'PRODUCER'
            getConfig() >> legacyConfig
        }
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def registry = new GcArtifactRegistry(graph, null, GcMode.ARTIFACT)
        def workDir = Files.createDirectories(tempDir.resolve('artifact-port-error'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = legacySuccessfulTask(producer, workDir, artifact)

        when:
        def update = registry.onTaskComplete(task)
        def deletions = registry.onDependencyClosed(producer)

        then:
        update.tracked == [artifact]
        update.deletions.empty
        deletions*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(artifact)
    }

    def 'workflow retention resolver failure remains conservative instead of becoming a liveness fallback'() {
        given:
        def legacyConfig = Mock(ProcessConfigV1) {
            getOutputs() >> { throw new IllegalStateException('broken workflow provenance') }
        }
        def producer = Mock(TaskProcessor) {
            getName() >> 'PRODUCER'
            getConfig() >> legacyConfig
        }
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def session = Mock(Session) {
            getOutputs() >> [result: new Object()]
            isSuccess() >> true
        }
        def registry = new GcArtifactRegistry(graph, session, GcMode.ARTIFACT)
        registry.setWorkflowOutputPorts(graph.outputPortsOf(producer), true)
        def workDir = Files.createDirectories(tempDir.resolve('workflow-port-error'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = legacySuccessfulTask(producer, workDir, artifact)

        when:
        def update = registry.onTaskComplete(task)
        def processClosed = registry.onDependencyClosed(producer)
        def flowDeletions = registry.onFlowComplete()

        then:
        update.keepReason == GcArtifactRegistry.KEEP_WORKFLOW_OUTPUT
        update.tracked.empty
        processClosed.empty
        flowDeletions.empty
        Files.exists(artifact)
    }


    def 'artifact pass-through without exact port provenance uses coarse process fallback instead of permanent retention'() {
        given:
        def producer = process('PRODUCER')
        def relay = process('RELAY')
        def graph = graph([[producer, relay]])
        def session = Mock(Session) {
            getOutputs() >> Collections.emptyMap()
            isSuccess() >> true
        }
        def registry = new GcArtifactRegistry(graph, session, GcMode.ARTIFACT)

        def producerWork = Files.createDirectories(tempDir.resolve('artifact-pass-source'))
        def source = Files.writeString(producerWork.resolve('source.txt'), 'source')
        def producerTask = successfulTask(producer, producerWork, [source] as Set<Path>)

        def relayWork = Files.createDirectories(tempDir.resolve('artifact-pass-relay'))
        def staged = relayWork.resolve('source.txt')
        Files.createSymbolicLink(staged, source)
        def relayTask = successfulTask(
            relay,
            relayWork,
            [staged] as Set<Path>,
            ['source.txt': source]
        )

        when:
        def producerUpdate = registry.onTaskComplete(producerTask)
        registry.onTaskPending(relayTask)
        def producerClosed = registry.onDependencyClosed(producer)
        def relayUpdate = registry.onTaskComplete(relayTask)
        def relayClosed = registry.onDependencyClosed(relay)

        then:
        producerUpdate.tracked == [source]
        producerClosed.empty
        relayUpdate.deletions.empty
        relayClosed*.path == [source]
        relayClosed*.status == [GcArtifactRegistry.DeleteStatus.DELETED]
        !Files.exists(source)

        when:
        def flowDeletions = registry.onFlowComplete()

        then:
        flowDeletions.empty
    }


    def 'missing artifacts become a terminal missing result without crashing'() {
        given:
        def producer = process('PRODUCER')
        def consumer = process('CONSUMER')
        def graph = graph([[producer, consumer]])
        def registry = new GcArtifactRegistry(graph)
        def workDir = Files.createDirectories(tempDir.resolve('producer'))
        def artifact = Files.writeString(workDir.resolve('result.txt'), 'result')
        def task = successfulTask(producer, workDir, [artifact] as Set<Path>)
        registry.onTaskComplete(task)
        Files.delete(artifact)

        when:
        def deletions = registry.onDependencyClosed(producer)
        def duplicate = registry.onDependencyClosed(producer)

        then:
        deletions*.status == [GcArtifactRegistry.DeleteStatus.MISSING]
        duplicate.empty
    }

    private TaskProcessor process(String name) {
        return Mock(TaskProcessor) {
            getName() >> name
        }
    }

    private TaskRun successfulTask(
        TaskProcessor process,
        Path workDir,
        Set<Path> outputs,
        Map<String,Path> inputFiles=Collections.emptyMap()
    ) {
        def config = Mock(TaskConfig) {
            getPublishDir() >> Collections.emptyList()
        }
        return Mock(TaskRun) {
            getProcessor() >> process
            getCached() >> false
            isSuccess() >> true
            getWorkDir() >> workDir
            getTargetDir() >> workDir
            hasTypedInputsOutputs() >> true
            getOutputFiles() >> outputs
            getInputFilesMap() >> inputFiles
            getConfig() >> config
            getName() >> process.name
        }
    }

    private TaskRun legacySuccessfulTask(TaskProcessor process, Path workDir, Path output) {
        def fileParam = Mock(FileOutParam)
        def config = Mock(TaskConfig) {
            getPublishDir() >> Collections.emptyList()
        }
        return Mock(TaskRun) {
            getProcessor() >> process
            getCached() >> false
            isSuccess() >> true
            getWorkDir() >> workDir
            getTargetDir() >> workDir
            hasTypedInputsOutputs() >> false
            getOutputsByType(FileOutParam) >> [(fileParam): output]
            getInputFilesMap() >> Collections.emptyMap()
            getConfig() >> config
            getName() >> process.name
        }
    }

    private static GcProcessGraph graph(List<List<TaskProcessor>> edges) {
        def consumers = new IdentityHashMap<TaskProcessor,Set<TaskProcessor>>()
        def producers = new IdentityHashMap<TaskProcessor,Set<TaskProcessor>>()

        edges.flatten().each { TaskProcessor process ->
            if( !consumers.containsKey(process) ) {
                consumers.put(process, identityProcessSet())
                producers.put(process, identityProcessSet())
            }
        }

        edges.each { pair ->
            def producer = pair[0]
            def consumer = pair[1]
            consumers.get(producer).add(consumer)
            producers.get(consumer).add(producer)
        }

        /* Unit tests construct the process projection directly rather than through
         * a real Nextflow DAG. Give each synthetic producer one output port so the
         * coarse lifecycle boundary matches the represented process edge instead
         * of looking like declaration-opaque global demand. */
        def outputPorts = new IdentityHashMap<TaskProcessor,Set<GcProcessGraph.OutputPort>>()
        def consumersByPort = new LinkedHashMap<GcProcessGraph.OutputPort,Set<TaskProcessor>>()
        def inputPorts = new IdentityHashMap<TaskProcessor,Set<GcProcessGraph.OutputPort>>()
        def routeKinds = new LinkedHashMap<GcProcessGraph.OutputPort,GcProcessGraph.PortRouteKind>()
        def runtimeClosure = new LinkedHashMap<GcProcessGraph.OutputPort,Set<groovyx.gpars.dataflow.operator.DataflowProcessor>>()

        consumers.keySet().each { TaskProcessor process ->
            outputPorts.put(process, new LinkedHashSet<GcProcessGraph.OutputPort>())
            inputPorts.put(process, new LinkedHashSet<GcProcessGraph.OutputPort>())
        }

        consumers.each { TaskProcessor producer, Set<TaskProcessor> downstream ->
            def port = new GcProcessGraph.OutputPort(producer, new Object())
            outputPorts.get(producer).add(port)
            consumersByPort.put(port, downstream)
            routeKinds.put(
                port,
                downstream.isEmpty()
                    ? GcProcessGraph.PortRouteKind.TERMINAL
                    : GcProcessGraph.PortRouteKind.DIRECT
            )
            runtimeClosure.put(port, Collections.emptySet())
            downstream.each { TaskProcessor consumer -> inputPorts.get(consumer).add(port) }
        }

        return new GcProcessGraph(
            consumers,
            producers,
            outputPorts,
            consumersByPort,
            inputPorts,
            routeKinds,
            runtimeClosure,
            new LinkedHashSet<GcProcessGraph.OutputPort>()
        )
    }

    private static GcProcessGraph globalSealGraph(TaskProcessor process) {
        def consumers = new IdentityHashMap<TaskProcessor,Set<TaskProcessor>>()
        def producers = new IdentityHashMap<TaskProcessor,Set<TaskProcessor>>()
        consumers.put(process, identityProcessSet())
        producers.put(process, identityProcessSet())

        def port = new GcProcessGraph.OutputPort(process, new Object())
        def outputPorts = new IdentityHashMap<TaskProcessor,Set<GcProcessGraph.OutputPort>>()
        outputPorts.put(process, new LinkedHashSet<GcProcessGraph.OutputPort>([port]))
        def consumersByPort = new LinkedHashMap<GcProcessGraph.OutputPort,Set<TaskProcessor>>()
        consumersByPort.put(port, identityProcessSet())
        def inputPorts = new IdentityHashMap<TaskProcessor,Set<GcProcessGraph.OutputPort>>()
        inputPorts.put(process, new LinkedHashSet<GcProcessGraph.OutputPort>())
        def routeKinds = new LinkedHashMap<GcProcessGraph.OutputPort,GcProcessGraph.PortRouteKind>()
        routeKinds.put(port, GcProcessGraph.PortRouteKind.FALLBACK)
        def runtimeClosure = new LinkedHashMap<GcProcessGraph.OutputPort,Set<groovyx.gpars.dataflow.operator.DataflowProcessor>>()
        runtimeClosure.put(port, Collections.emptySet())

        return new GcProcessGraph(
            consumers,
            producers,
            outputPorts,
            consumersByPort,
            inputPorts,
            routeKinds,
            runtimeClosure,
            new LinkedHashSet<GcProcessGraph.OutputPort>([port])
        )
    }


    private static Set<TaskProcessor> identityProcessSet(Collection<TaskProcessor> values=Collections.emptyList()) {
        def result = Collections.newSetFromMap(new IdentityHashMap<TaskProcessor,Boolean>())
        result.addAll(values)
        return result
    }
}
