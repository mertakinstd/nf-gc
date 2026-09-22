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

import java.io.File
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.Comparator
import java.util.IdentityHashMap
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.function.BiConsumer

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.extension.CH
import nextflow.file.FileHelper
import nextflow.processor.PublishDir
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.ProcessConfigV1
import nextflow.script.params.EachInParam
import nextflow.script.params.FileOutParam
import nextflow.script.params.InputsList
import nextflow.script.params.OutParam
import nextflow.script.params.TupleOutParam

/**
 * Tracks Nextflow-owned task outputs and reclaims them according to the
 * configured GC policy.
 *
 * Detection, ownership, retention, provenance, and deletion are concrete-Path
 * based in both modes. Process mode advances the same Path state at producer/
 * consumer process boundaries; artifact mode can advance it at exact task/runtime
 * last-use boundaries. Unknown liveness falls back conservatively without changing
 * the artifact identity or eventual retention policy.
 */
@Slf4j
@CompileStatic
final class GcArtifactRegistry {

    static final String KEEP_PUBLISH_DIR = 'PUBLISH_DIR'
    static final String KEEP_WORKFLOW_OUTPUT = 'WORKFLOW_OUTPUT'
    static final String KEEP_CACHED = 'CACHED'
    static final String KEEP_TARGET_DIR = 'TARGET_DIR'
    static final String KEEP_UNKNOWN = 'UNKNOWN'

    static enum DeleteStatus {
        DELETED,
        MISSING,
        FAILED
    }

    @CompileStatic
    static final class Update {
        final TaskProcessor process
        final List<Path> tracked
        final List<Path> held
        final List<DeletionResult> deletions
        final String keepReason

        Update(
            TaskProcessor process,
            Collection<Path> tracked,
            Collection<Path> held,
            Collection<DeletionResult> deletions,
            String keepReason=null
        ) {
            this.process = process
            this.tracked = Collections.unmodifiableList(new ArrayList<Path>(tracked))
            this.held = Collections.unmodifiableList(new ArrayList<Path>(held))
            this.deletions = Collections.unmodifiableList(new ArrayList<DeletionResult>(deletions))
            this.keepReason = keepReason
        }
    }

    @CompileStatic
    static final class DeletionResult {
        final TaskProcessor process
        final Path path
        final DeleteStatus status
        final String error

        DeletionResult(TaskProcessor process, Path path, DeleteStatus status, String error=null) {
            this.process = process
            this.path = path
            this.status = status
            this.error = error
        }
    }

    private static enum DemandKind {
        TERMINAL,
        DIRECT,
        FALLBACK
    }

    @CompileStatic
    private static final class PortDemand {
        final GcProcessGraph.OutputPort port
        final DemandKind kind
        final Set<TaskProcessor> expectedConsumers
        boolean ambiguous

        PortDemand(
            GcProcessGraph.OutputPort port,
            DemandKind kind,
            Collection<TaskProcessor> expectedConsumers
        ) {
            this.port = port
            this.kind = kind
            final Set<TaskProcessor> copy = newIdentityProcessSet()
            if( expectedConsumers != null )
                copy.addAll(expectedConsumers)
            this.expectedConsumers = copy
        }
    }

    @CompileStatic
    private static final class StagedInputMatch {
        final Path source
        final boolean exact

        StagedInputMatch(Path source, boolean exact) {
            this.source = source
            this.exact = exact
        }
    }

    @CompileStatic
    private static final class PublicationProtection {
        final Set<Path> retained = new LinkedHashSet<>()
        final Set<Path> deferred = new LinkedHashSet<>()
    }

    /**
     * Mode-independent output classification for one successful task. The two GC
     * modes consume the same ownership/retention facts and differ only in the
     * lifecycle evidence they require before reclaiming an eligible Path.
     */
    @CompileStatic
    private static final class TaskOutputPlan {
        Map<Path, Set<GcProcessGraph.OutputPort>> outputPortsByPath
        final Set<Path> nonIndependentOutputs = new LinkedHashSet<>()
        final List<Path> ownedOutputs = new ArrayList<>()
        final Map<Path,Path> exactPassThrough = new LinkedHashMap<>()
        final Set<Path> ambiguousPassThroughSources = new LinkedHashSet<>()
        final Set<Path> workflowRetainedDeclarations = new LinkedHashSet<>()
        final List<Path> lifecycleCandidates = new ArrayList<>()
        final List<Path> reclaimableOwnedOutputs = new ArrayList<>()
        PublicationProtection publication = new PublicationProtection()
        boolean workflowFallback
        boolean targetDirUnsafe
        boolean filesystemFallback
    }

    @CompileStatic
    private static final class ArtifactState {
        final Path path
        TaskProcessor owner
        boolean producerComplete
        boolean workflowOutput
        boolean unknownRetained
        boolean processFallback
        boolean fallbackRequiresGlobalSeal
        final Map<GcProcessGraph.OutputPort, PortDemand> demands = new LinkedHashMap<>()
        final Set<TaskProcessor> completedConsumers = newIdentityProcessSet()

        ArtifactState(Path path) {
            this.path = path
        }
    }

    private final GcProcessGraph graph
    private final Session session
    private final GcMode gcMode
    /* Optional test-only lifecycle sink; GcObserver remains the trace I/O owner. */
    private final BiConsumer<String,String> traceSink

    /* Shared concrete-artifact ownership and hold state. */
    private final Map<Path, TaskProcessor> ownerByArtifact = new LinkedHashMap<>()
    private final Set<Path> globalHeldArtifacts = new LinkedHashSet<>()

    private final Set<Path> publishedSources = new LinkedHashSet<>()
    private final Map<Path, Path> deferredPublicationBackings = new LinkedHashMap<>()
    private final Map<Path, Set<TaskRun>> taskHoldsByArtifact = new LinkedHashMap<>()
    private final Map<TaskRun, Set<Path>> heldArtifactsByTask = new IdentityHashMap<>()
    private final Map<Path, Set<TaskProcessor>> processHoldsByArtifact = new LinkedHashMap<>()
    private final Map<TaskProcessor, Set<Path>> heldArtifactsByProcess = new IdentityHashMap<>()

    private final Set<TaskProcessor> dependencyClosed = newIdentityProcessSet()
    private final Set<TaskProcessor> terminatedProcesses = newIdentityProcessSet()
    private final Set<GcProcessGraph.OutputPort> outputPortsClosed = new LinkedHashSet<>()
    private final Set<TaskRun> pendingTasks = newIdentityTaskSet()
    private final Set<TaskRun> completedTasks = newIdentityTaskSet()
    private final Set<TaskProcessor> unsafeReservationConsumers = newIdentityProcessSet()

    /* Shared concrete-Path liveness state. */
    private final Map<Path, ArtifactState> artifactStates = new LinkedHashMap<>()
    private final Map<GcProcessGraph.OutputPort, Set<Path>> artifactsByPort = new LinkedHashMap<>()
    private final Set<GcProcessGraph.OutputPort> workflowOutputPorts = new LinkedHashSet<>()
    private final Set<GcProcessGraph.OutputPort> runtimeSealedPorts = new LinkedHashSet<>()
    private boolean workflowOutputProvenanceTrusted
    private boolean flowComplete

    GcArtifactRegistry(
        GcProcessGraph graph,
        Session session=null,
        GcMode gcMode=GcMode.PROCESS,
        BiConsumer<String,String> traceSink=null
    ) {
        if( graph == null )
            throw new IllegalArgumentException('Process graph must not be null')
        if( gcMode == null )
            throw new IllegalArgumentException('GC mode must not be null')
        this.graph = graph
        this.session = session
        this.gcMode = gcMode
        this.traceSink = traceSink
        this.workflowOutputProvenanceTrusted = !hasWorkflowOutputs()
    }


    synchronized void setWorkflowOutputPorts(Collection<GcProcessGraph.OutputPort> ports, boolean trusted) {
        workflowOutputPorts.clear()
        if( ports != null )
            workflowOutputPorts.addAll(ports)
        workflowOutputProvenanceTrusted = trusted
    }

    synchronized List<DeletionResult> onRuntimeRouteSealed(GcProcessGraph.OutputPort port) {
        if( port == null )
            return Collections.emptyList()
        if( !runtimeSealedPorts.add(port) )
            return Collections.emptyList()

        tracePortEvent('RUNTIME_ROUTE_SEALED', port)

        final Set<Path> paths = artifactsByPort.get(port)
        if( paths == null || paths.isEmpty() )
            return Collections.emptyList()
        return Collections.unmodifiableList(deleteReadyArtifacts(new ArrayList<Path>(paths)))
    }

    synchronized void onFilePublish(Path source) {
        final Path path = normalize(source, null)
        if( path == null )
            return

        publishedSources.add(path)
        final Path backing = deferredPublicationBackings.remove(path)
        if( backing != null )
            retainPublishedArtifact(backing)
    }

    /**
     * Nextflow 26.04.x drains the publish pool before notifying flow completion.
     * On a successful flow, any unresolved async saveAs candidate that never
     * emitted FilePublishEvent is therefore proven unpublished. Both clocks also
     * gain a global future-demand seal because the workflow and operators have
     * completed.
     */
    synchronized List<DeletionResult> onFlowComplete() {
        if( !successfulFlowCompletion() )
            return Collections.emptyList()

        /*
         * Nextflow 26.04.6 reaches TraceObserverV2.onFlowComplete only after
         * Session.await() has joined process/operator activity and Session.destroy()
         * has drained the finalize/publish pools. This is the coarse global seal
         * used by both modes for otherwise-unobservable demand and deferred async
         * publication decisions.
         */
        flowComplete = true
        deferredPublicationBackings.clear()
        globalHeldArtifacts.clear()

        if( !workflowOutputProvenanceTrusted )
            return Collections.emptyList()
        return Collections.unmodifiableList(deleteReadyArtifacts(new ArrayList<Path>(artifactStates.keySet())))
    }

    /**
     * Reserve concrete input Paths as soon as Nextflow has resolved a pending
     * task. Detection is mode-independent; only the later liveness clock differs.
     * onTaskStart calls the same helper as a defensive fallback for runtimes or
     * tests that do not expose the pending callback.
     */
    synchronized void onTaskPending(TaskRun task) {
        reserveTaskInputs(task)
    }

    synchronized void onTaskStart(TaskRun task) {
        reserveTaskInputs(task)
    }

    private void reserveTaskInputs(TaskRun task) {
        if( task == null || task.processor == null || !graph.contains(task.processor) )
            return
        if( !pendingTasks.add(task) )
            return

        final Set<Path> sources
        try {
            sources = taskReservationSources(task)
        }
        catch( Exception e ) {
            markConsumerReservationUnsafe(task.processor)
            log.warn "nf-gc could not resolve pending task inputs for ${task.name}; liveness will fall back conservatively", e
            return
        }

        for( Path source : sources )
            addTaskHold(source, task)
    }

    synchronized Update onTaskComplete(TaskRun task) {
        if( task == null || task.processor == null || !graph.contains(task.processor) )
            return emptyUpdate(task?.processor, KEEP_UNKNOWN)

        final TaskProcessor process = task.processor
        if( !completedTasks.add(task) )
            return emptyUpdate(process)
        pendingTasks.remove(task)

        final Path workDir = normalize(task.workDir, null)
        if( workDir == null ) {
            final List<Path> held = promoteAllTaskHolds(task, process)
            return update(process, Collections.<Path>emptyList(), held, Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)
        }

        final Map<Path,Path> stagedInputs
        try {
            stagedInputs = stagedInputSources(task, workDir)
        }
        catch( Throwable e ) {
            final List<Path> held = promoteAllTaskHolds(task, process)
            log.warn "nf-gc could not resolve staged inputs for task ${task.name}; keeping outputs", e
            return update(process, Collections.<Path>emptyList(), held, Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)
        }

        if( task.cached ) {
            final List<DeletionResult> released = completeTaskConsumer(task, stagedInputs.values(), true)
            return update(process, Collections.<Path>emptyList(), Collections.<Path>emptyList(), released, KEEP_CACHED)
        }

        if( !task.isSuccess() ) {
            /* A failed task may be retried after its process operator has already
             * stopped. Promote concrete reservations to the coarse process fallback
             * rather than inventing retry lifecycle state here. */
            final List<Path> held = promoteAllTaskHolds(task, process)
            return update(process, Collections.<Path>emptyList(), held, Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)
        }

        final TaskOutputPlan plan
        try {
            plan = classifyTaskOutputs(task, workDir, stagedInputs)
        }
        catch( Throwable e ) {
            final List<Path> held = promoteAllTaskHolds(task, process)
            log.warn "nf-gc could not classify output artifacts for task ${task.name}; keeping outputs", e
            return update(process, Collections.<Path>emptyList(), held, Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)
        }

        if( plan.workflowFallback ) {
            /* Exact workflow-output provenance is a retention fact shared by both
             * modes. If it cannot be resolved for this task, keep the task outputs
             * and any pass-through backing Paths rather than allowing the mode's
             * liveness clock to guess. */
            for( Path source : plan.exactPassThrough.values() )
                retainArtifactConservatively(source)

            final List<DeletionResult> released = completeTaskConsumer(task, stagedInputs.values(), true)
            return update(process, Collections.<Path>emptyList(), Collections.<Path>emptyList(), released, KEEP_WORKFLOW_OUTPUT)
        }

        return applyTaskPlan(task, stagedInputs, plan)
    }

    /** Release reservations held by a cached consumer task. */
    synchronized List<DeletionResult> onTaskCached(TaskRun task) {
        if( task == null || task.processor == null )
            return Collections.emptyList()
        if( !graph.contains(task.processor) )
            return Collections.emptyList()
        if( !completedTasks.add(task) )
            return Collections.emptyList()
        pendingTasks.remove(task)

        Collection<Path> sources = Collections.<Path>emptySet()
        try {
            sources = taskReservationSources(task)
        }
        catch( Exception e ) {
            markConsumerReservationUnsafe(task.processor)
            log.warn "nf-gc could not resolve cached task inputs for ${task.name}; liveness will remain conservative", e
        }
        return Collections.unmodifiableList(completeTaskConsumer(task, sources, true))
    }

    /**
     * Resolve the mode-independent ownership and retention facts for a successful
     * task. No liveness/closure decision is made here.
     */
    private TaskOutputPlan classifyTaskOutputs(TaskRun task, Path workDir, Map<Path,Path> stagedInputs) {
        final TaskOutputPlan plan = new TaskOutputPlan()
        final Set<Path> outputs = outputPaths(task)
        final Set<Path> realizedOutputs = realizedOutputPaths(outputs, workDir)
        plan.nonIndependentOutputs.addAll(nonIndependentOutputPaths(realizedOutputs, workDir))
        plan.filesystemFallback = !plan.nonIndependentOutputs.isEmpty()

        for( Path artifact : realizedOutputs ) {
            final StagedInputMatch staged = stagedInputMatch(artifact, stagedInputs)
            if( staged != null ) {
                if( staged.exact && !plan.nonIndependentOutputs.contains(artifact) )
                    plan.exactPassThrough.put(artifact, staged.source)
                else {
                    plan.ambiguousPassThroughSources.add(staged.source)
                    plan.filesystemFallback = true
                }
                continue
            }
            plan.ownedOutputs.add(artifact)
        }

        for( Path source : plan.ambiguousPassThroughSources )
            retainArtifactConservatively(source)

        /* Only task-owned outputs can establish a new symlink backing relation.
         * Staged pass-through symlinks are represented by their exact source Path. */
        if( retainSymlinkBackingArtifacts(plan.ownedOutputs, workDir) )
            plan.filesystemFallback = true

        final boolean workflowOutputsPresent = hasWorkflowOutputs()
        if( workflowOutputsPresent && !workflowOutputProvenanceTrusted ) {
            plan.workflowFallback = true
            return plan
        }

        final boolean workflowPortsRequired = workflowOutputsPresent
        try {
            plan.outputPortsByPath = outputPortsByPath(task, workDir)
        }
        catch( Throwable e ) {
            if( workflowPortsRequired ) {
                log.warn "nf-gc could not resolve workflow-output provenance for task ${task.name}; keeping task outputs", e
                plan.workflowFallback = true
                return plan
            }
            log.warn "nf-gc could not resolve output-port provenance for task ${task.name}; this artifact will use coarse process liveness", e
            plan.outputPortsByPath = null
        }

        if( workflowOutputsPresent ) {
            if( plan.outputPortsByPath == null ) {
                plan.workflowFallback = true
                return plan
            }

            final Set<Path> declarations = new LinkedHashSet<>(plan.ownedOutputs)
            declarations.addAll(plan.exactPassThrough.keySet())
            for( Path declaration : declarations ) {
                final Set<GcProcessGraph.OutputPort> ports = plan.outputPortsByPath.get(declaration)
                if( ports == null || ports.isEmpty() ) {
                    plan.workflowFallback = true
                    return plan
                }
                if( intersects(ports, workflowOutputPorts) )
                    plan.workflowRetainedDeclarations.add(declaration)
            }
        }

        final Path targetDir = normalize(task.targetDir, null)
        plan.targetDirUnsafe = targetDir == null || !targetDir.equals(workDir)

        final Set<Path> publicationCandidates = new LinkedHashSet<>(plan.ownedOutputs)
        publicationCandidates.addAll(plan.exactPassThrough.keySet())
        plan.publication = publicationProtection(task, publicationCandidates)

        /* This is the single owned-Path eligibility set consumed by both modes.
         * Liveness may differ after this point; permanent retention must not. */
        for( Path artifact : plan.ownedOutputs ) {
            if( plan.publication.retained.contains(artifact) || plan.nonIndependentOutputs.contains(artifact) )
                continue
            plan.lifecycleCandidates.add(artifact)
            if( !plan.workflowRetainedDeclarations.contains(artifact) )
                plan.reclaimableOwnedOutputs.add(artifact)
        }
        return plan
    }

    private Update applyTaskPlan(TaskRun task, Map<Path,Path> stagedInputs, TaskOutputPlan plan) {
        final TaskProcessor process = task.processor
        final List<Path> newlyHeld = new ArrayList<>()
        boolean unknownPassThrough = false

        /* Extend concrete pass-through lineage before releasing this task's input
         * reservation. This detection is shared by both clocks. */
        for( Map.Entry<Path,Path> entry : plan.exactPassThrough.entrySet() ) {
            final Path declaration = entry.key
            final Path source = entry.value
            final Set<GcProcessGraph.OutputPort> ports = plan.outputPortsByPath?.get(declaration)

            if( plan.workflowRetainedDeclarations.contains(declaration) ) {
                retainWorkflowArtifact(source)
                continue
            }

            if( ports != null && !ports.isEmpty() ) {
                extendArtifactDemand(source, ports)
            }
            else {
                unknownPassThrough = true
                if( addCoarseLifecycleHold(source, process) )
                    newlyHeld.add(source)
            }

            if( plan.targetDirUnsafe || plan.publication.retained.contains(declaration) )
                retainArtifactConservatively(source)
            else if( plan.publication.deferred.contains(declaration) )
                deferPublication(declaration, source)
        }

        final List<DeletionResult> releasedInputDeletions = completeTaskConsumer(task, stagedInputs.values(), true)

        if( !plan.ownedOutputs.isEmpty() && plan.lifecycleCandidates.isEmpty() ) {
            final String reason = plan.filesystemFallback ? KEEP_UNKNOWN : KEEP_PUBLISH_DIR
            return update(process, Collections.<Path>emptyList(), newlyHeld, releasedInputDeletions, reason)
        }

        if( plan.targetDirUnsafe )
            return update(process, Collections.<Path>emptyList(), newlyHeld, releasedInputDeletions, KEEP_TARGET_DIR)

        final List<Path> newlyTracked = new ArrayList<>()
        final List<Path> candidates = new ArrayList<>()
        final boolean hasWorkflowRetained = !plan.workflowRetainedDeclarations.isEmpty()

        for( Path artifact : plan.reclaimableOwnedOutputs ) {
            final ArtifactState state = stateFor(artifact)
            if( state.owner != null && !state.owner.is(process) ) {
                log.warn "nf-gc observed conflicting artifact ownership for ${artifact}; keeping output"
                retainArtifactConservatively(artifact)
                continue
            }

            if( state.owner == null ) {
                state.owner = process
                ownerByArtifact.put(artifact, process)
                newlyTracked.add(artifact)
            }
            state.producerComplete = true

            final Set<GcProcessGraph.OutputPort> ports = plan.outputPortsByPath?.get(artifact)
            if( ports == null || ports.isEmpty() ) {
                state.processFallback = true
                state.fallbackRequiresGlobalSeal = processMayRequireGlobalSeal(process)
            }
            else {
                addArtifactDemands(state, ports)
            }

            if( plan.publication.deferred.contains(artifact) )
                deferPublication(artifact, artifact)
            candidates.add(artifact)
        }

        final List<DeletionResult> deletions = new ArrayList<>(releasedInputDeletions)
        deletions.addAll(deleteReadyArtifacts(candidates))

        final String keepReason
        if( hasWorkflowRetained && !plan.lifecycleCandidates.isEmpty() && deletions.isEmpty() )
            keepReason = KEEP_WORKFLOW_OUTPUT
        else if( unknownPassThrough || plan.filesystemFallback )
            keepReason = KEEP_UNKNOWN
        else
            keepReason = null

        return update(process, newlyTracked, newlyHeld, deletions, keepReason)
    }

    /**
     * When exact Path-to-port provenance is unavailable, only that concrete Path
     * falls back to producer-wide process closure. If the producer exposes demand
     * outside the process projection, that Path alone requires the global seal.
     */
    private boolean processMayRequireGlobalSeal(TaskProcessor process) {
        if( process == null )
            return true

        final Set<GcProcessGraph.OutputPort> ports = graph.outputPortsOf(process)
        if( ports.isEmpty() )
            return true

        for( GcProcessGraph.OutputPort port : ports ) {
            if( graph.requiresGlobalSeal(port) )
                return true
            if( graph.routeKind(port) == GcProcessGraph.PortRouteKind.FALLBACK
                    && graph.consumersOf(port).isEmpty() )
                return true
        }
        return false
    }

    synchronized List<DeletionResult> onProcessTerminated(TaskProcessor process) {
        if( process == null || !graph.contains(process) )
            return Collections.emptyList()
        if( !terminatedProcesses.add(process) )
            return Collections.emptyList()

        final Set<Path> candidates = new LinkedHashSet<>()
        for( GcProcessGraph.OutputPort port : graph.outputPortsOf(process) ) {
            final Set<Path> paths = artifactsByPort.get(port)
            if( paths != null )
                candidates.addAll(paths)
        }
        for( Map.Entry<Path,TaskProcessor> entry : ownerByArtifact.entrySet() ) {
            if( entry.value != null && entry.value.is(process) )
                candidates.add(entry.key)
        }
        return Collections.unmodifiableList(deleteReadyArtifacts(candidates))
    }

    synchronized List<DeletionResult> onDependencyClosed(TaskProcessor process) {
        if( process == null || !graph.contains(process) )
            return Collections.emptyList()

        if( !dependencyClosed.add(process) )
            return Collections.emptyList()

        final Set<Path> released = new LinkedHashSet<>(releaseProcessHoldsPaths(process))
        for( ArtifactState state : artifactStates.values() ) {
            if( state.processFallback && state.owner != null && state.owner.is(process) )
                released.add(state.path)
        }
        return Collections.unmodifiableList(deleteReadyArtifacts(released))
    }

    synchronized List<DeletionResult> onOutputClosed(GcProcessGraph.OutputPort port) {
        if( port == null )
            return Collections.emptyList()
        if( !outputPortsClosed.add(port) )
            return Collections.emptyList()

        tracePortEvent('OUTPUT_PORT_CLOSED', port)

        final Set<Path> paths = artifactsByPort.get(port)
        if( paths == null || paths.isEmpty() )
            return Collections.emptyList()
        return Collections.unmodifiableList(deleteReadyArtifacts(new ArrayList<Path>(paths)))
    }

    private boolean hasWorkflowOutputs() {
        try {
            return session != null && session.outputs != null && !session.outputs.isEmpty()
        }
        catch( Throwable e ) {
            log.warn 'nf-gc could not resolve workflow outputs; keeping task outputs', e
            return true
        }
    }

    private void extendArtifactDemand(Path rawSource, Collection<GcProcessGraph.OutputPort> ports) {
        final Path source = normalize(rawSource, null)
        if( source == null || ports == null || ports.isEmpty() )
            return

        addArtifactDemands(stateFor(source), ports)
    }

    private void addArtifactDemands(ArtifactState state, Collection<GcProcessGraph.OutputPort> ports) {
        if( state == null || ports == null )
            return

        for( GcProcessGraph.OutputPort port : ports ) {
            if( port == null || state.demands.containsKey(port) )
                continue

            final DemandKind kind = demandKind(port)
            final PortDemand demand = new PortDemand(port, kind, graph.consumersOf(port))
            state.demands.put(port, demand)
            traceArtifactRoute(state.path, demand)

            Set<Path> paths = artifactsByPort.get(port)
            if( paths == null ) {
                paths = new LinkedHashSet<Path>()
                artifactsByPort.put(port, paths)
            }
            paths.add(state.path)

        }
        markAmbiguousExactRoutes(state)
    }

    private void traceArtifactRoute(Path path, PortDemand demand) {
        if( traceSink == null || path == null || demand == null )
            return

        final String detail = [
            path,
            demand.port,
            graph.routeKind(demand.port).name(),
            demand.kind.name(),
            processNames(demand.expectedConsumers),
            graph.requiresGlobalSeal(demand.port)
        ].join('\t')
        traceSink.accept('ARTIFACT_ROUTE', detail)
    }

    private void tracePortEvent(String event, GcProcessGraph.OutputPort port) {
        if( traceSink == null || event == null || port == null )
            return

        final String detail = [
            port,
            graph.routeKind(port).name(),
            processNames(graph.consumersOf(port)),
            graph.requiresGlobalSeal(port)
        ].join('\t')
        traceSink.accept(event, detail)
    }

    private static String processNames(Collection<TaskProcessor> processes) {
        if( processes == null || processes.isEmpty() )
            return ''

        final List<String> names = new ArrayList<>()
        for( TaskProcessor process : processes ) {
            if( process != null )
                names.add(process.name ?: '<unknown>')
        }
        Collections.sort(names)
        return names.join(',')
    }

    private static void markAmbiguousExactRoutes(ArtifactState state) {
        final Map<TaskProcessor,Integer> memberships = new IdentityHashMap<>()
        for( PortDemand demand : state.demands.values() ) {
            if( demand.kind == DemandKind.TERMINAL )
                continue
            for( TaskProcessor consumer : demand.expectedConsumers )
                memberships.put(consumer, (memberships.get(consumer) ?: 0) + 1)
        }

        for( PortDemand demand : state.demands.values() ) {
            if( demand.kind != DemandKind.DIRECT )
                continue
            for( TaskProcessor consumer : demand.expectedConsumers ) {
                if( (memberships.get(consumer) ?: 0) > 1 ) {
                    demand.ambiguous = true
                    break
                }
            }
        }
    }

    private void markConsumerReservationUnsafe(TaskProcessor consumer) {
        if( consumer != null )
            unsafeReservationConsumers.add(consumer)
    }

    private boolean hasUnsafeReservationConsumer(Collection<TaskProcessor> consumers) {
        if( consumers == null || consumers.isEmpty() )
            return false
        for( TaskProcessor consumer : consumers ) {
            if( unsafeReservationConsumers.contains(consumer) )
                return true
        }
        return false
    }

    private boolean consumerCanReuseInput(TaskProcessor consumer) {
        if( consumer == null )
            return true
        try {
            if( consumer.config instanceof ProcessConfigV1 ) {
                final ProcessConfigV1 config = (ProcessConfigV1)consumer.config
                final InputsList inputs = config.getInputs()
                return inputs != null && !inputs.ofType(EachInParam).isEmpty()
            }
            /* Unknown/typed input models stay conservative until their reuse
             * semantics can be proven from a stable Nextflow API. */
            return true
        }
        catch( Exception e ) {
            log.warn "nf-gc could not classify input reuse for process ${consumer.name}; exact artifact GC will fall back to output-port closure", e
            return true
        }
    }

    private boolean exactConsumersSafe(Collection<TaskProcessor> consumers) {
        if( consumers == null || consumers.isEmpty() )
            return false
        for( TaskProcessor consumer : consumers ) {
            if( unsafeReservationConsumers.contains(consumer) || consumerCanReuseInput(consumer) )
                return false
        }
        return true
    }

    private DemandKind demandKind(GcProcessGraph.OutputPort port) {
        if( graph.isTerminal(port) )
            return DemandKind.TERMINAL

        /* A value can legally be reused by future task instances. */
        if( CH.isValue(port.channel) )
            return DemandKind.FALLBACK

        if( !exactConsumersSafe(graph.consumersOf(port)) )
            return DemandKind.FALLBACK

        switch( graph.routeKind(port) ) {
        case GcProcessGraph.PortRouteKind.DIRECT:
            return DemandKind.DIRECT
        case GcProcessGraph.PortRouteKind.TERMINAL:
            return DemandKind.TERMINAL
        default:
            return DemandKind.FALLBACK
        }
    }

    private List<DeletionResult> completeTaskConsumer(
        TaskRun task,
        Collection<Path> observedSources,
        boolean satisfyDemand
    ) {
        final Set<Path> candidates = new LinkedHashSet<>()
        final Set<Path> held = heldArtifactsByTask.get(task)
        if( held != null )
            candidates.addAll(held)
        if( observedSources != null ) {
            for( Path raw : observedSources ) {
                final Path path = normalize(raw, null)
                if( path != null )
                    candidates.add(path)
            }
        }

        for( Path path : candidates ) {
            final ArtifactState state = stateFor(path)
            if( satisfyDemand )
                state.completedConsumers.add(task.processor)
        }

        releaseTaskHolds(task)
        return deleteReadyArtifacts(candidates)
    }

    private boolean lifecycleReady(ArtifactState state) {
        if( !workflowOutputProvenanceTrusted )
            return false
        if( state == null || state.owner == null || !state.producerComplete || state.workflowOutput || state.unknownRetained )
            return false
        if( isHeld(state.path) )
            return false
        if( flowComplete )
            return true
        if( state.processFallback ) {
            if( state.fallbackRequiresGlobalSeal )
                return false
            return dependencyClosed.contains(state.owner)
        }
        if( state.demands.isEmpty() )
            return false

        for( PortDemand demand : state.demands.values() ) {
            final boolean ready = gcMode == GcMode.PROCESS
                ? processDemandReady(demand)
                : artifactDemandReady(state, demand)
            if( !ready )
                return false
        }
        return true
    }

    /** Process mode keeps Path identity but advances it only at process boundaries. */
    private boolean processDemandReady(PortDemand demand) {
        if( graph.requiresGlobalSeal(demand.port) )
            return false

        switch( graph.routeKind(demand.port) ) {
        case GcProcessGraph.PortRouteKind.TERMINAL:
            return terminatedProcesses.contains(demand.port.producer)

        case GcProcessGraph.PortRouteKind.DIRECT:
            return outputPortsClosed.contains(demand.port)

        case GcProcessGraph.PortRouteKind.FALLBACK:
            /* An operator-backed route needs both its process boundary and the
             * observed operator closure. If either proof is unavailable, the
             * successful global flow seal remains the safe fallback. */
            if( !runtimeSealedPorts.contains(demand.port) )
                return false
            if( demand.expectedConsumers.isEmpty() )
                return terminatedProcesses.contains(demand.port.producer)
            return outputPortsClosed.contains(demand.port)

        default:
            return false
        }
    }

    private boolean artifactDemandReady(ArtifactState state, PortDemand demand) {
        /* Topic-backed or declaration-opaque ports can carry legal demand outside
         * both the process DAG and the observed operator closure. Nextflow exposes
         * no topic-specific completion event here, so only FLOW_COMPLETE seals them. */
        if( graph.requiresGlobalSeal(demand.port) )
            return false

        switch( demand.kind ) {
        case DemandKind.TERMINAL:
            return true

        case DemandKind.DIRECT:
            if( !demand.ambiguous
                    && !hasUnsafeReservationConsumer(demand.expectedConsumers)
                    && !demand.expectedConsumers.isEmpty()
                    && state.completedConsumers.containsAll(demand.expectedConsumers) )
                return true
            return outputPortsClosed.contains(demand.port)

        case DemandKind.FALLBACK:
            if( demand.expectedConsumers.isEmpty() )
                return runtimeSealedPorts.contains(demand.port)

            return (!hasUnsafeReservationConsumer(demand.expectedConsumers)
                    && runtimeSealedPorts.contains(demand.port)
                    && hasConcreteRuntimeConsumer(state, demand))
                || outputPortsClosed.contains(demand.port)

        default:
            return outputPortsClosed.contains(demand.port)
        }
    }

    private List<DeletionResult> deleteReadyArtifacts(Collection<Path> rawCandidates) {
        if( rawCandidates == null || rawCandidates.isEmpty() )
            return Collections.emptyList()

        final Set<Path> unique = new LinkedHashSet<>()
        for( Path raw : rawCandidates ) {
            final Path path = normalize(raw, null)
            if( path != null )
                unique.add(path)
        }

        final List<Path> ordered = new ArrayList<>(unique)
        Collections.sort(ordered, new Comparator<Path>() {
            @Override
            int compare(Path left, Path right) {
                return Integer.compare(right.nameCount, left.nameCount)
            }
        })

        final List<DeletionResult> results = new ArrayList<>()
        for( Path path : ordered ) {
            final ArtifactState state = artifactStates.get(path)
            if( !lifecycleReady(state) || hasTrackedDescendant(path) )
                continue

            final DeletionResult result = deleteArtifact(state.owner, path)
            results.add(result)
            if( result.status != DeleteStatus.FAILED )
                removeArtifactState(state)
        }
        return results
    }

    private static boolean hasConcreteRuntimeConsumer(ArtifactState state, PortDemand demand) {
        if( demand.expectedConsumers.isEmpty() )
            return true
        for( TaskProcessor consumer : demand.expectedConsumers ) {
            if( state.completedConsumers.contains(consumer) )
                return true
        }
        return false
    }

    private boolean hasTrackedDescendant(Path parent) {
        for( ArtifactState other : artifactStates.values() ) {
            if( other.path.equals(parent) || !other.path.startsWith(parent) )
                continue
            return true
        }
        return false
    }

    private void removeArtifactState(ArtifactState state) {
        artifactStates.remove(state.path)
        ownerByArtifact.remove(state.path)

        for( GcProcessGraph.OutputPort port : state.demands.keySet() ) {
            final Set<Path> paths = artifactsByPort.get(port)
            if( paths == null )
                continue
            paths.remove(state.path)
            if( paths.isEmpty() )
                artifactsByPort.remove(port)
        }
    }

    private static boolean intersects(Collection<GcProcessGraph.OutputPort> left, Collection<GcProcessGraph.OutputPort> right) {
        if( left == null || right == null || left.isEmpty() || right.isEmpty() )
            return false
        for( GcProcessGraph.OutputPort port : left ) {
            if( right.contains(port) )
                return true
        }
        return false
    }

    private ArtifactState stateFor(Path rawPath) {
        final Path path = normalize(rawPath, null)
        if( path == null )
            throw new IllegalArgumentException('Artifact path must not be null')
        ArtifactState state = artifactStates.get(path)
        if( state == null ) {
            state = new ArtifactState(path)
            artifactStates.put(path, state)
        }
        return state
    }

    private PublicationProtection publicationProtection(TaskRun task, Collection<Path> artifacts) {
        final PublicationProtection protection = new PublicationProtection()
        if( artifacts == null || artifacts.isEmpty() )
            return protection

        final List<PublishDir> directives
        try {
            directives = task.config != null ? task.config.getPublishDir() : null
        }
        catch( Throwable e ) {
            log.warn "nf-gc could not resolve publishDir for task ${task.name}; keeping outputs", e
            protection.retained.addAll(artifacts)
            return protection
        }

        if( directives == null || directives.isEmpty() )
            return protection

        final Path sourceDir = normalize(task.targetDir, null)
        if( sourceDir == null ) {
            protection.retained.addAll(artifacts)
            return protection
        }

        try {
            for( PublishDir directive : directives ) {
                if( directive == null ) {
                    protection.retained.addAll(artifacts)
                    continue
                }
                /* Nextflow PublishDir.apply returns immediately when disabled;
                 * disabled publication is exact negative evidence, not a reason
                 * to retain outputs. */
                if( !directive.isEnabled() )
                    continue

                final String pattern = directive.getPattern()
                final PathMatcher matcher = pattern != null && !pattern.isEmpty()
                    ? FileHelper.getPathMatcherFor('glob:' + pattern, sourceDir.getFileSystem())
                    : null
                final boolean hasSaveAs = directive.getSaveAs() != null
                final boolean publicationObservedInline = hasSaveAs && publishesInline(directive)

                for( Path artifact : artifacts ) {
                    final Path relative = sourceDir.relativize(artifact)
                    if( matcher != null && !matcher.matches(relative) )
                        continue

                    if( !hasSaveAs ) {
                        protection.retained.add(artifact)
                        continue
                    }

                    if( publishedSources.contains(artifact) ) {
                        protection.retained.add(artifact)
                        continue
                    }

                    if( !publicationObservedInline ) {
                        /* Without failOnError, a publish failure can leave no
                         * FilePublishEvent while the workflow still succeeds.
                         * In that configuration absence of an event is not exact
                         * negative evidence, so keep the artifact conservatively. */
                        if( directive.isFailOnError() )
                            protection.deferred.add(artifact)
                        else
                            protection.retained.add(artifact)
                    }
                }
            }
        }
        catch( Throwable e ) {
            log.warn "nf-gc could not classify publishDir artifacts for task ${task.name}; keeping outputs", e
            protection.retained.clear()
            protection.retained.addAll(artifacts)
            protection.deferred.clear()
        }
        finally {
            /* Inline publication evidence is only needed while this task is
             * classified. Async events arriving afterwards are matched against
             * deferredPublicationBackings by onFilePublish. */
            publishedSources.removeAll(artifacts)
        }

        protection.deferred.removeAll(protection.retained)
        return protection
    }

    private static boolean publishesInline(PublishDir directive) {
        final PublishDir.Mode mode = directive.getMode()
        return mode == PublishDir.Mode.LINK || mode == PublishDir.Mode.SYMLINK || mode == PublishDir.Mode.RELLINK
    }

    private void deferPublication(Path rawDeclaration, Path rawBacking) {
        final Path declaration = normalize(rawDeclaration, null)
        final Path backing = normalize(rawBacking, null)
        if( declaration == null || backing == null )
            return
        deferredPublicationBackings.put(declaration, backing)
    }

    private void retainPublishedArtifact(Path rawBacking) {
        retainArtifactConservatively(rawBacking)
    }

    private void retainArtifactConservatively(Path rawPath) {
        final Path path = normalize(rawPath, null)
        if( path != null )
            stateFor(path).unknownRetained = true
    }

    private void retainWorkflowArtifact(Path rawPath) {
        final Path path = normalize(rawPath, null)
        if( path != null )
            stateFor(path).workflowOutput = true
    }

    private boolean successfulFlowCompletion() {
        try {
            return session != null && session.isSuccess()
        }
        catch( Exception e ) {
            log.warn 'nf-gc could not confirm successful flow completion; deferred artifacts will be kept', e
            return false
        }
    }

    /**
     * Resolve legacy task file outputs to their top-level process output channel.
     * Typed outputs remain conservative until Nextflow exposes an equally exact
     * declaration-to-path mapping for that model.
     */
    private Map<Path, Set<GcProcessGraph.OutputPort>> outputPortsByPath(TaskRun task, Path workDir) {
        if( task.hasTypedInputsOutputs() )
            return null
        if( !(task.processor.config instanceof ProcessConfigV1) )
            return null

        final ProcessConfigV1 config = (ProcessConfigV1)task.processor.config
        final Map<FileOutParam, GcProcessGraph.OutputPort> portsByParam = new IdentityHashMap<>()

        for( OutParam output : config.getOutputs() ) {
            final Object channel = output.getOutChannel()
            if( channel == null )
                continue

            final GcProcessGraph.OutputPort port = graph.outputPort(task.processor, channel)
            if( port == null )
                return null

            if( output instanceof FileOutParam ) {
                portsByParam.put((FileOutParam)output, port)
            }
            else if( output instanceof TupleOutParam ) {
                final TupleOutParam tuple = (TupleOutParam)output
                for( Object inner : tuple.inner ) {
                    if( inner instanceof FileOutParam )
                        portsByParam.put((FileOutParam)inner, port)
                }
            }
        }

        final Map resolved = task.getOutputsByType(FileOutParam)
        final Map<Path, Set<GcProcessGraph.OutputPort>> result = new LinkedHashMap<>()
        for( Object rawEntry : resolved.entrySet() ) {
            final Map.Entry entry = (Map.Entry)rawEntry
            final FileOutParam param = (FileOutParam)entry.key
            final GcProcessGraph.OutputPort port = portsByParam.get(param)
            if( port == null )
                return null

            final List<Path> values = new ArrayList<>()
            appendOutputPaths(entry.value, values)
            for( Path raw : values ) {
                final Path path = normalize(raw, workDir)
                if( path == null )
                    continue
                Set<GcProcessGraph.OutputPort> ports = result.get(path)
                if( ports == null ) {
                    ports = new LinkedHashSet<GcProcessGraph.OutputPort>()
                    result.put(path, ports)
                }
                ports.add(port)
            }
        }
        return result
    }

    private static Set<Path> realizedOutputPaths(Collection<Path> outputs, Path workDir) {
        final Set<Path> result = new LinkedHashSet<>()
        if( outputs == null || workDir == null )
            return result

        for( Path raw : outputs ) {
            final Path artifact = normalize(raw, workDir)
            if( artifact == null || artifact == workDir || !artifact.startsWith(workDir) )
                continue
            result.add(artifact)
        }
        return result
    }

    /**
     * Return realized outputs that cannot be treated as independent deletion
     * units using Path identity alone. This deliberately stays filesystem-local:
     * lexical ancestor/descendant outputs, outputs reached through a symlink
     * ancestor, and declared outputs backing another declared symlink are kept
     * conservatively rather than introducing a hierarchy/alias graph.
     */
    private static Set<Path> nonIndependentOutputPaths(Collection<Path> paths, Path workDir) {
        if( paths == null || paths.isEmpty() )
            return Collections.emptySet()

        final List<Path> values = new ArrayList<>(paths)
        final Set<Path> result = new LinkedHashSet<>()

        for( int i=0; i<values.size(); i++ ) {
            final Path left = values.get(i)
            if( hasSymlinkAncestor(left, workDir) )
                result.add(left)

            for( int j=i+1; j<values.size(); j++ ) {
                final Path right = values.get(j)
                if( pathTreesOverlap(left, right) ) {
                    result.add(left)
                    result.add(right)
                }
            }
        }

        for( Path link : values ) {
            if( !Files.isSymbolicLink(link) )
                continue

            final Path target = symbolicLinkTarget(link)
            if( target == null ) {
                /* The alias relationship is unknowable; keep this task's
                 * realized outputs rather than guessing which one backs it. */
                result.addAll(values)
                continue
            }

            for( Path other : values ) {
                if( other.equals(link) )
                    continue
                if( pathTreesOverlap(target, other) ) {
                    result.add(link)
                    result.add(other)
                }
            }
        }
        return result
    }

    private static boolean pathTreesOverlap(Path left, Path right) {
        if( left == null || right == null )
            return false
        return left.equals(right) || left.startsWith(right) || right.startsWith(left)
    }

    private boolean retainSymlinkBackingArtifacts(Collection<Path> outputs, Path workDir) {
        final Set<Path> targets = symlinkBackedTargets(outputs, workDir)
        if( targets.isEmpty() )
            return false

        boolean retained = false
        for( Path source : new ArrayList<Path>(ownerByArtifact.keySet()) ) {
            if( !overlapsAnyPhysicalTree(source, targets) )
                continue
            retainArtifactConservatively(source)
            retained = true
        }
        return retained
    }

    private static Set<Path> symlinkBackedTargets(Collection<Path> outputs, Path workDir) {
        if( outputs == null || outputs.isEmpty() || workDir == null )
            return Collections.emptySet()

        final Set<Path> result = new LinkedHashSet<>()
        for( Path artifact : outputs ) {
            if( artifact == null )
                continue
            if( !Files.isSymbolicLink(artifact) && !hasSymlinkAncestor(artifact, workDir) )
                continue

            try {
                result.add(artifact.toRealPath())
            }
            catch( Exception ignored ) {
                final Path target = symlinkBackedTarget(artifact, workDir)
                if( target != null )
                    result.add(target)
            }
        }
        return result
    }

    private static Path symlinkBackedTarget(Path artifact, Path workDir) {
        if( Files.isSymbolicLink(artifact) )
            return symbolicLinkTarget(artifact)

        final Path root = normalize(workDir, null)
        Path current = artifact.parent
        while( current != null && current.startsWith(root) && !current.equals(root) ) {
            if( Files.isSymbolicLink(current) ) {
                final Path target = symbolicLinkTarget(current)
                return target != null ? target.resolve(current.relativize(artifact)).normalize() : null
            }
            current = current.parent
        }
        return null
    }

    private static boolean overlapsAnyPhysicalTree(Path source, Collection<Path> targets) {
        for( Path target : targets ) {
            if( physicalTreesOverlap(source, target) )
                return true
        }
        return false
    }

    private static boolean physicalTreesOverlap(Path left, Path right) {
        if( pathTreesOverlap(left, right) )
            return true
        try {
            return pathTreesOverlap(left.toRealPath(), right.toRealPath())
        }
        catch( Exception ignored ) {
            return false
        }
    }

    private static boolean hasSymlinkAncestor(Path artifact, Path workDir) {
        if( artifact == null || workDir == null )
            return true

        final Path root = normalize(workDir, null)
        Path current = artifact.parent
        while( current != null && current.startsWith(root) && !current.equals(root) ) {
            if( Files.isSymbolicLink(current) )
                return true
            current = current.parent
        }
        return false
    }

    private static Path symbolicLinkTarget(Path link) {
        try {
            return link.toRealPath()
        }
        catch( Exception ignored ) {
            try {
                final Path raw = Files.readSymbolicLink(link)
                return normalize(raw, link.parent)
            }
            catch( Exception unresolved ) {
                return null
            }
        }
    }

    private static StagedInputMatch stagedInputMatch(Path artifact, Map<Path,Path> stagedInputs) {
        if( artifact == null || stagedInputs == null || stagedInputs.isEmpty() )
            return null

        final Path exact = stagedInputs.get(artifact)
        if( exact != null )
            return new StagedInputMatch(exact, true)

        Path bestStaged = null
        Path bestSource = null
        for( Map.Entry<Path,Path> entry : stagedInputs.entrySet() ) {
            final Path staged = entry.key
            if( staged == null || !artifact.startsWith(staged) || artifact.equals(staged) )
                continue
            if( bestStaged == null || staged.nameCount > bestStaged.nameCount ) {
                bestStaged = staged
                bestSource = entry.value
            }
        }
        return bestSource != null ? new StagedInputMatch(bestSource, false) : null
    }

    private static Set<Path> outputPaths(TaskRun task) {
        final Set<Path> result = new LinkedHashSet<>()

        if( task.hasTypedInputsOutputs() ) {
            if( task.outputFiles != null )
                result.addAll(task.outputFiles)
            return result
        }

        final Map outputs = task.getOutputsByType(FileOutParam)
        final List<Path> values = new ArrayList<>()
        for( Object value : outputs.values() )
            appendOutputPaths(value, values)
        result.addAll(values)
        return result
    }

    private static void appendOutputPaths(Object value, Collection<Path> result) {
        if( value instanceof Path ) {
            result.add((Path)value)
        }
        else if( value instanceof Collection ) {
            for( Object item : (Collection)value ) {
                if( item instanceof Path )
                    result.add((Path)item)
                else if( item != null )
                    throw new IllegalArgumentException("Unknown output file object [${item.class.name}]: ${item}")
            }
        }
        else if( value != null ) {
            throw new IllegalArgumentException("Unknown output file object [${value.class.name}]: ${value}")
        }
    }

    /**
     * Concrete Paths already resolved by Nextflow for this task. File inputs use
     * inputFilesMap values, which are the source paths, so pending reservation
     * does not depend on the task work directory. Paths carried as ordinary
     * values are preserved in TaskRun.inputs and are reserved directly.
     */
    private static Set<Path> taskReservationSources(TaskRun task) {
        final Set<Path> result = new LinkedHashSet<Path>()
        final Map<String,Path> fileInputs = task.inputFilesMap
        if( fileInputs != null ) {
            for( Path raw : fileInputs.values() ) {
                final Path path = normalize(raw, null)
                if( path != null )
                    result.add(path)
            }
        }

        final Map inputs = task.inputs
        if( inputs == null || inputs.isEmpty() )
            return result

        final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object,Boolean>())
        for( Object value : inputs.values() )
            appendInputPaths(value, result, seen)
        return result
    }

    private static void appendInputPaths(Object value, Collection<Path> result, Set<Object> seen) {
        if( value == null )
            return
        if( value instanceof Path ) {
            final Path path = normalize((Path)value, null)
            if( path != null )
                result.add(path)
            return
        }
        if( value instanceof File ) {
            final Path path = normalize(((File)value).toPath(), null)
            if( path != null )
                result.add(path)
            return
        }
        if( value instanceof Map ) {
            if( !seen.add(value) )
                return
            for( Object rawEntry : ((Map)value).entrySet() ) {
                final Map.Entry entry = (Map.Entry)rawEntry
                appendInputPaths(entry.key, result, seen)
                appendInputPaths(entry.value, result, seen)
            }
            return
        }
        if( value instanceof Collection ) {
            if( !seen.add(value) )
                return
            for( Object item : (Collection)value )
                appendInputPaths(item, result, seen)
            return
        }
        if( value.class.isArray() && !value.class.componentType.isPrimitive() ) {
            if( !seen.add(value) )
                return
            for( Object item : (Object[])value )
                appendInputPaths(item, result, seen)
        }
    }

    private static Map<Path,Path> stagedInputSources(TaskRun task, Path workDir) {
        final Map<Path,Path> result = new LinkedHashMap<>()
        final Map<String,Path> inputs = task.inputFilesMap
        if( inputs == null )
            return result

        for( Map.Entry<String,Path> entry : inputs.entrySet() ) {
            final String stagedName = entry.key
            if( stagedName == null )
                continue

            final Path staged = workDir.resolve(stagedName).toAbsolutePath().normalize()
            final Path source = normalize(entry.value, null)
            if( source != null )
                result.put(staged, source)
        }
        return result
    }

    private boolean addTaskHold(Path rawSource, TaskRun holder) {
        final Path source = normalize(rawSource, null)
        if( source == null || holder == null )
            return false

        Set<TaskRun> holders = taskHoldsByArtifact.get(source)
        if( holders == null ) {
            holders = newIdentityTaskSet()
            taskHoldsByArtifact.put(source, holders)
        }

        Set<Path> paths = heldArtifactsByTask.get(holder)
        if( paths == null ) {
            paths = new LinkedHashSet<Path>()
            heldArtifactsByTask.put(holder, paths)
        }
        paths.add(source)
        return holders.add(holder)
    }

    /** Hold one concrete Path at a conservative process/global boundary. */
    private boolean addCoarseLifecycleHold(Path rawSource, TaskProcessor holder) {
        final Path source = normalize(rawSource, null)
        if( source == null || holder == null )
            return false

        if( processMayRequireGlobalSeal(holder) ) {
            if( flowComplete )
                return false
            return globalHeldArtifacts.add(source)
        }
        return addProcessHold(source, holder)
    }

    private boolean addProcessHold(Path rawSource, TaskProcessor holder) {
        final Path source = normalize(rawSource, null)
        if( source == null || holder == null || dependencyClosed.contains(holder) )
            return false

        Set<TaskProcessor> holders = processHoldsByArtifact.get(source)
        if( holders == null ) {
            holders = newIdentityProcessSet()
            processHoldsByArtifact.put(source, holders)
        }

        Set<Path> paths = heldArtifactsByProcess.get(holder)
        if( paths == null ) {
            paths = new LinkedHashSet<Path>()
            heldArtifactsByProcess.put(holder, paths)
        }
        paths.add(source)
        return holders.add(holder)
    }

    private List<Path> promoteAllTaskHolds(TaskRun task, TaskProcessor process) {
        final Set<Path> paths = heldArtifactsByTask.get(task)
        if( paths == null || paths.isEmpty() )
            return Collections.emptyList()

        final List<Path> promoted = new ArrayList<>()
        for( Path path : paths ) {
            if( addCoarseLifecycleHold(path, process) )
                promoted.add(path)
        }
        releaseTaskHolds(task)
        return promoted
    }

    private Set<TaskProcessor> releaseTaskHolds(TaskRun holder) {
        final Set<Path> paths = heldArtifactsByTask.remove(holder)
        if( paths == null || paths.isEmpty() )
            return Collections.emptySet()

        final Set<TaskProcessor> owners = newIdentityProcessSet()
        for( Path path : paths ) {
            final Set<TaskRun> holders = taskHoldsByArtifact.get(path)
            if( holders != null ) {
                holders.remove(holder)
                if( holders.isEmpty() )
                    taskHoldsByArtifact.remove(path)
            }

            final TaskProcessor owner = ownerByArtifact.get(path)
            if( owner != null )
                owners.add(owner)
        }
        return owners
    }

    private Set<Path> releaseProcessHoldsPaths(TaskProcessor holder) {
        final Set<Path> paths = heldArtifactsByProcess.remove(holder)
        if( paths == null || paths.isEmpty() )
            return Collections.emptySet()

        final Set<Path> released = new LinkedHashSet<>(paths)
        for( Path path : paths ) {
            final Set<TaskProcessor> holders = processHoldsByArtifact.get(path)
            if( holders != null ) {
                holders.remove(holder)
                if( holders.isEmpty() )
                    processHoldsByArtifact.remove(path)
            }
        }
        return released
    }

    private boolean isHeld(Path path) {
        if( !flowComplete && globalHeldArtifacts.contains(path) )
            return true
        if( deferredPublicationBackings.containsValue(path) )
            return true
        final Set<TaskRun> taskHolders = taskHoldsByArtifact.get(path)
        if( taskHolders != null && !taskHolders.isEmpty() )
            return true
        final Set<TaskProcessor> processHolders = processHoldsByArtifact.get(path)
        return processHolders != null && !processHolders.isEmpty()
    }

    private static DeletionResult deleteArtifact(TaskProcessor process, Path path) {
        if( !Files.exists(path, LinkOption.NOFOLLOW_LINKS) )
            return new DeletionResult(process, path, DeleteStatus.MISSING)

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file)
                    return FileVisitResult.CONTINUE
                }

                @Override
                FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if( exc != null )
                        throw exc
                    Files.deleteIfExists(dir)
                    return FileVisitResult.CONTINUE
                }
            })
            return new DeletionResult(process, path, DeleteStatus.DELETED)
        }
        catch( IOException e ) {
            return new DeletionResult(process, path, DeleteStatus.FAILED, e.message)
        }
    }

    private static Update emptyUpdate(TaskProcessor process, String keepReason=null) {
        return update(
            process,
            Collections.<Path>emptyList(),
            Collections.<Path>emptyList(),
            Collections.<DeletionResult>emptyList(),
            keepReason
        )
    }

    private static Update update(
        TaskProcessor process,
        Collection<Path> tracked,
        Collection<Path> held,
        Collection<DeletionResult> deletions,
        String keepReason=null
    ) {
        return new Update(process, tracked, held, deletions, keepReason)
    }

    private static Path normalize(Path path, Path base) {
        if( path == null )
            return null
        final Path resolved = !path.isAbsolute() && base != null ? base.resolve(path) : path
        return resolved.toAbsolutePath().normalize()
    }

    private static Set<TaskProcessor> newIdentityProcessSet() {
        return Collections.newSetFromMap(new IdentityHashMap<TaskProcessor, Boolean>())
    }

    private static Set<TaskRun> newIdentityTaskSet() {
        return Collections.newSetFromMap(new IdentityHashMap<TaskRun, Boolean>())
    }
}
