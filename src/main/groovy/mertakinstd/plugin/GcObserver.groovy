/*
 * Copyright 2025, Seqera Labs
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

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.ArrayList
import java.util.Collections
import java.util.List

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.trace.event.TaskEvent
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.FilePublishEvent
import nextflow.trace.event.WorkflowOutputEvent

/**
 * Observes the workflow lifecycle used by nf-gc and coordinates process
 * dependency closure with conservative artifact reclamation.
 */
@Slf4j
@CompileStatic
class GcObserver implements TraceObserverV2 {

    private Path traceFile
    private Session session
    private GcProcessGraph processGraph
    private GcDependencyState dependencyState
    private GcArtifactRegistry artifactRegistry

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        configureTestTrace(session)
        initializeTrace()
        record('FLOW_CREATE')
        log.debug 'nf-gc flow created'
    }

    @Override
    void onFlowBegin() {
        record('FLOW_BEGIN')

        if( session == null )
            throw new IllegalStateException('nf-gc flow began before session initialization')

        this.processGraph = GcProcessGraph.from(session.dag)
        this.dependencyState = new GcDependencyState(processGraph)
        this.artifactRegistry = new GcArtifactRegistry(processGraph, session)
        recordGraph(processGraph)
        log.debug "nf-gc flow begun with ${processGraph.processes.size()} processes"
    }

    @Override
    void onProcessCreate(TaskProcessor process) {
        record('PROCESS_CREATE', process.name)
        log.debug "nf-gc process created: ${process.name}"
    }

    @Override
    void onProcessTerminate(TaskProcessor process) {
        record('PROCESS_TERMINATE', process.name)

        if( dependencyState == null || processGraph == null || !processGraph.contains(process) ) {
            record('DEPENDENCY_UNKNOWN', process.name)
            log.warn "nf-gc ignored termination for process outside the dependency graph: ${process.name}"
            return
        }

        for( TaskProcessor closed : dependencyState.onProcessTerminate(process) ) {
            record('DEPENDENCY_CLOSED', closed.name)
            recordDeletions(artifactRegistry.onDependencyClosed(closed))
        }

        log.debug "nf-gc process terminated: ${process.name}"
    }


    @Override
    void onTaskStart(TaskEvent event) {
        if( artifactRegistry == null )
            return

        final TaskRun task = event?.handler?.task
        if( task != null )
            artifactRegistry.onTaskStart(task)
    }

    @Override
    void onTaskComplete(TaskEvent event) {
        if( artifactRegistry == null )
            return

        final TaskRun task = event?.handler?.task
        if( task == null )
            return

        final GcArtifactRegistry.Update update = artifactRegistry.onTaskComplete(task)
        if( update.keepReason != null )
            record('ARTIFACT_KEEP', "${task.processor?.name ?: '<unknown>'}\t${update.keepReason}".toString())

        for( Path path : update.tracked )
            record('ARTIFACT_TRACKED', "${task.processor.name}\t${path}".toString())

        for( Path path : update.held )
            record('ARTIFACT_HOLD', "${task.processor.name}\t${path}".toString())

        recordDeletions(update.deletions)
    }

    @Override
    void onTaskCached(TaskEvent event) {
        final TaskRun task = event?.handler?.task
        if( task != null )
            record('ARTIFACT_KEEP', "${task.processor?.name ?: '<unknown>'}\t${GcArtifactRegistry.KEEP_CACHED}".toString())
    }

    @Override
    void onFilePublish(FilePublishEvent event) {
        if( artifactRegistry != null && event != null )
            artifactRegistry.onFilePublish(event.source)
        record('FILE_PUBLISH')
        log.debug "nf-gc file published: ${event}"
    }

    @Override
    void onWorkflowOutput(WorkflowOutputEvent event) {
        final String detail = event == null
            ? null
            : "${event.name ?: '<unnamed>'}\t${event.value ?: ''}".toString()
        record('WORKFLOW_OUTPUT', detail)
        log.debug "nf-gc workflow output completed: ${event}"
    }

    @Override
    void onFlowComplete() {
        record('FLOW_COMPLETE')
        log.debug 'nf-gc flow completed'
    }

    /**
     * Enables a test-only lifecycle trace through Nextflow's standard env
     * scope. This is intentionally not part of the nf-gc public config API.
     */
    private void configureTestTrace(Session session) {
        final Object envConfig = session.config.get('env')
        if( !(envConfig instanceof Map) )
            return

        final Object enabled = ((Map) envConfig).get('NF_GC_TEST_TRACE')
        if( enabled == null || !Boolean.parseBoolean(enabled.toString()) )
            return

        this.traceFile = session.workDir.parent.resolve('nf-gc-events.tsv')
    }


    private void recordDeletions(Collection<GcArtifactRegistry.DeletionResult> results) {
        for( GcArtifactRegistry.DeletionResult result : results ) {
            final String detail = "${result.process.name}\t${result.path}".toString()
            switch( result.status ) {
            case GcArtifactRegistry.DeleteStatus.DELETED:
                record('ARTIFACT_DELETED', detail)
                break
            case GcArtifactRegistry.DeleteStatus.MISSING:
                record('ARTIFACT_MISSING', detail)
                break
            case GcArtifactRegistry.DeleteStatus.FAILED:
                record('ARTIFACT_DELETE_FAILED', "${detail}\t${result.error ?: ''}".toString())
                log.warn "nf-gc failed to delete artifact ${result.path}: ${result.error ?: 'unknown error'}"
                break
            }
        }
    }

    private void recordGraph(GcProcessGraph graph) {
        if( traceFile == null )
            return

        final List<String> edges = new ArrayList<>()
        for( TaskProcessor producer : graph.processes ) {
            for( TaskProcessor consumer : graph.consumersOf(producer) ) {
                edges.add("${producer.name}\t${consumer.name}".toString())
            }
        }

        Collections.sort(edges)
        for( String edge : edges )
            record('GRAPH_EDGE', edge)
    }

    private void initializeTrace() {
        if( traceFile == null )
            return

        Files.writeString(
            traceFile,
            '',
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        )
    }

    private synchronized void record(String event, String detail = null) {
        if( traceFile == null )
            return

        final String line = detail == null
            ? "${event}\n"
            : "${event}\t${detail}\n"

        Files.writeString(
            traceFile,
            line,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        )
    }
}
