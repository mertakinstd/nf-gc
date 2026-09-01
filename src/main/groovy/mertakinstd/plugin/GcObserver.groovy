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

    @Override
    void onFlowCreate(Session session) {
        log.debug 'nf-gc flow created'
    }

    @Override
    void onFlowBegin() {
        log.debug 'nf-gc flow begun'
    }

    @Override
    void onProcessCreate(TaskProcessor process) {
        log.debug "nf-gc process created: ${process.name}"
    }

    @Override
    void onProcessTerminate(TaskProcessor process) {
        log.debug "nf-gc process terminated: ${process.name}"
    }

    @Override
    void onFilePublish(FilePublishEvent event) {
        log.debug "nf-gc file published: ${event}"
    }

    @Override
    void onWorkflowOutput(WorkflowOutputEvent event) {
        log.debug "nf-gc workflow output completed: ${event}"
    }

    @Override
    void onFlowComplete() {
        log.debug 'nf-gc flow completed'
    }
}
