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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskProcessor
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.FilePublishEvent
import nextflow.trace.event.WorkflowOutputEvent

/**
 * Observes the workflow lifecycle used by nf-gc.
 *
 * This class deliberately performs no garbage collection yet. The first
 * implementation milestone is to verify the lifecycle boundaries exposed by
 * TraceObserverV2 before attaching deletion semantics to them.
 */
@Slf4j
@CompileStatic
class GcObserver implements TraceObserverV2 {

    private Path traceFile

    @Override
    void onFlowCreate(Session session) {
        configureTestTrace(session)
        initializeTrace()
        record('FLOW_CREATE')
        log.debug 'nf-gc flow created'
    }

    @Override
    void onFlowBegin() {
        record('FLOW_BEGIN')
        log.debug 'nf-gc flow begun'
    }

    @Override
    void onProcessCreate(TaskProcessor process) {
        record('PROCESS_CREATE', process.name)
        log.debug "nf-gc process created: ${process.name}"
    }

    @Override
    void onProcessTerminate(TaskProcessor process) {
        record('PROCESS_TERMINATE', process.name)
        log.debug "nf-gc process terminated: ${process.name}"
    }

    @Override
    void onFilePublish(FilePublishEvent event) {
        record('FILE_PUBLISH')
        log.debug "nf-gc file published: ${event}"
    }

    @Override
    void onWorkflowOutput(WorkflowOutputEvent event) {
        record('WORKFLOW_OUTPUT')
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
