package mertakinstd.plugin

import java.nio.file.Files
import java.nio.file.Path

import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
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

        return new GcProcessGraph(consumers, producers)
    }

    private static Set<TaskProcessor> identityProcessSet() {
        return Collections.newSetFromMap(new IdentityHashMap<TaskProcessor,Boolean>())
    }
}
