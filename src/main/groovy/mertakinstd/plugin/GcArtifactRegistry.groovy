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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.file.FileHelper
import nextflow.processor.PublishDir
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.params.FileOutParam

/**
 * Tracks Nextflow-owned task outputs that can be reclaimed once their producer
 * process becomes dependency-closed.
 *
 * The policy is deliberately conservative:
 *   - only successful, non-cached Nextflow task outputs are considered;
 *   - only artifacts owned by the task work directory are considered;
 *   - published artifacts, workflow outputs, terminal outputs, storeDir and
 *     unknown cases are kept;
 *   - staged inputs never acquire ownership by being re-emitted.
 *
 * Staged inputs are provisionally pinned from task start until task completion.
 * If a staged input is re-emitted, that pin is promoted to the relay process
 * and remains until the relay becomes dependency-closed. This makes cleanup
 * independent of observer callback ordering.
 */
@Slf4j
@CompileStatic
final class GcArtifactRegistry {

    static final String KEEP_PUBLISH_DIR = 'PUBLISH_DIR'
    static final String KEEP_WORKFLOW_OUTPUT = 'WORKFLOW_OUTPUT'
    static final String KEEP_TERMINAL = 'TERMINAL'
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

    private final GcProcessGraph graph
    private final Session session

    private final Map<TaskProcessor, Set<Path>> tracked = new IdentityHashMap<>()
    private final Map<Path, TaskProcessor> ownerByArtifact = new LinkedHashMap<>()
    private final Set<Path> publishedSources = new LinkedHashSet<>()

    private final Map<Path, Set<TaskRun>> taskHoldsByArtifact = new LinkedHashMap<>()
    private final Map<TaskRun, Set<Path>> heldArtifactsByTask = new IdentityHashMap<>()

    private final Map<Path, Set<TaskProcessor>> processHoldsByArtifact = new LinkedHashMap<>()
    private final Map<TaskProcessor, Set<Path>> heldArtifactsByProcess = new IdentityHashMap<>()

    private final Set<TaskProcessor> dependencyClosed = newIdentityProcessSet()
    private final Set<TaskRun> startedTasks = newIdentityTaskSet()
    private final Set<TaskRun> completedTasks = newIdentityTaskSet()

    GcArtifactRegistry(GcProcessGraph graph, Session session=null) {
        if( graph == null )
            throw new IllegalArgumentException('Process graph must not be null')
        this.graph = graph
        this.session = session
    }

    synchronized void onFilePublish(Path source) {
        final Path path = normalize(source, null)
        if( path != null )
            publishedSources.add(path)
    }

    /**
     * Provisionally pin every staged input source before task execution can
     * race with process termination callbacks. These pins are released on task
     * completion unless the input is actually re-emitted downstream.
     */
    synchronized void onTaskStart(TaskRun task) {
        if( task == null || task.processor == null || !graph.contains(task.processor) )
            return
        if( !startedTasks.add(task) )
            return

        final Path workDir = normalize(task.workDir, null)
        if( workDir == null )
            return

        final Map<Path,Path> stagedInputs
        try {
            stagedInputs = stagedInputSources(task, workDir)
        }
        catch( Throwable e ) {
            log.warn "nf-gc could not resolve staged inputs for task ${task.name}; no input artifact will be reclaimed from this observation", e
            return
        }

        for( Path source : stagedInputs.values() )
            addTaskHold(source, task)
    }

    synchronized Update onTaskComplete(TaskRun task) {
        if( task == null || task.processor == null || !graph.contains(task.processor) )
            return emptyUpdate(task?.processor, KEEP_UNKNOWN)

        final TaskProcessor process = task.processor
        if( !completedTasks.add(task) )
            return emptyUpdate(process)

        final Path workDir = normalize(task.workDir, null)
        if( workDir == null ) {
            final List<DeletionResult> released = releaseTaskHoldsAndDelete(task)
            return update(process, Collections.<Path>emptyList(), Collections.<Path>emptyList(), released, KEEP_UNKNOWN)
        }

        final Map<Path,Path> stagedInputs
        try {
            stagedInputs = stagedInputSources(task, workDir)
        }
        catch( Throwable e ) {
            /*
             * We cannot classify pass-through outputs without the staging map.
             * Keep the task outputs and retain any provisional input pins until
             * process dependency closure by promoting them conservatively.
             */
            final List<Path> held = promoteAllTaskHolds(task, process)
            log.warn "nf-gc could not resolve staged inputs for task ${task.name}; keeping outputs", e
            return update(process, Collections.<Path>emptyList(), held, Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)
        }

        if( task.cached ) {
            final List<DeletionResult> released = releaseTaskHoldsAndDelete(task)
            return update(process, Collections.<Path>emptyList(), Collections.<Path>emptyList(), released, KEEP_CACHED)
        }

        if( !task.isSuccess() ) {
            final List<DeletionResult> released = releaseTaskHoldsAndDelete(task)
            return update(process, Collections.<Path>emptyList(), Collections.<Path>emptyList(), released, KEEP_UNKNOWN)
        }

        final Set<Path> outputs
        try {
            outputs = outputPaths(task)
        }
        catch( Throwable e ) {
            final List<Path> held = promoteAllTaskHolds(task, process)
            log.warn "nf-gc could not resolve output artifacts for task ${task.name}; keeping outputs", e
            return update(process, Collections.<Path>emptyList(), held, Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)
        }

        final List<Path> newlyHeld = new ArrayList<>()
        final List<Path> ownedOutputs = new ArrayList<>()

        for( Path raw : outputs ) {
            final Path artifact = normalize(raw, workDir)
            if( artifact == null || artifact == workDir || !artifact.startsWith(workDir) )
                continue

            final Path stagedSource = stagedInputs.get(artifact)
            if( stagedSource != null ) {
                if( addProcessHold(stagedSource, process) )
                    newlyHeld.add(stagedSource)
                continue
            }

            ownedOutputs.add(artifact)
        }

        /*
         * Promotion happens before provisional task holds are released, so a
         * pass-through source can never become momentarily collectible between
         * the two states.
         */
        final List<DeletionResult> releasedInputDeletions = releaseTaskHoldsAndDelete(task)

        /*
         * Workflow outputs are a publication surface separate from process
         * publishDir. Until artifact-level provenance for session.outputs is
         * modelled, any configured workflow output pins intermediate cleanup
         * for the whole run.
         */
        if( hasWorkflowOutputs() )
            return update(process, Collections.<Path>emptyList(), newlyHeld, releasedInputDeletions, KEEP_WORKFLOW_OUTPUT)

        final Set<Path> publishedOutputs = publicationProtectedOutputs(task, ownedOutputs)
        final List<Path> collectibleOutputs = new ArrayList<>()
        for( Path artifact : ownedOutputs ) {
            if( !publishedOutputs.contains(artifact) )
                collectibleOutputs.add(artifact)
        }

        if( !ownedOutputs.isEmpty() && collectibleOutputs.isEmpty() )
            return update(process, Collections.<Path>emptyList(), newlyHeld, releasedInputDeletions, KEEP_PUBLISH_DIR)

        if( graph.consumersOf(process).isEmpty() )
            return update(process, Collections.<Path>emptyList(), newlyHeld, releasedInputDeletions, KEEP_TERMINAL)

        final Path targetDir = normalize(task.targetDir, null)
        if( targetDir == null || !targetDir.equals(workDir) )
            return update(process, Collections.<Path>emptyList(), newlyHeld, releasedInputDeletions, KEEP_TARGET_DIR)

        Set<Path> processArtifacts = tracked.get(process)
        if( processArtifacts == null ) {
            processArtifacts = new LinkedHashSet<Path>()
            tracked.put(process, processArtifacts)
        }

        final List<Path> newlyTracked = new ArrayList<>()
        for( Path artifact : collectibleOutputs ) {
            if( processArtifacts.add(artifact) ) {
                ownerByArtifact.put(artifact, process)
                newlyTracked.add(artifact)
            }
        }

        final List<DeletionResult> deletions = new ArrayList<>(releasedInputDeletions)
        if( dependencyClosed.contains(process) )
            deletions.addAll(deleteReady(process))

        return update(process, newlyTracked, newlyHeld, deletions)
    }

    synchronized List<DeletionResult> onDependencyClosed(TaskProcessor process) {
        if( process == null || !graph.contains(process) )
            return Collections.emptyList()

        if( !dependencyClosed.add(process) )
            return Collections.emptyList()

        final Set<TaskProcessor> candidates = newIdentityProcessSet()
        candidates.add(process)
        candidates.addAll(releaseProcessHolds(process))

        final List<DeletionResult> results = new ArrayList<>()
        for( TaskProcessor candidate : candidates ) {
            if( dependencyClosed.contains(candidate) )
                results.addAll(deleteReady(candidate))
        }
        return Collections.unmodifiableList(results)
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

    private Set<Path> publicationProtectedOutputs(TaskRun task, Collection<Path> artifacts) {
        if( artifacts == null || artifacts.isEmpty() )
            return Collections.emptySet()

        final List<PublishDir> directives
        try {
            directives = task.config != null ? task.config.getPublishDir() : null
        }
        catch( Throwable e ) {
            log.warn "nf-gc could not resolve publishDir for task ${task.name}; keeping outputs", e
            return new LinkedHashSet<Path>(artifacts)
        }

        if( directives == null || directives.isEmpty() )
            return Collections.emptySet()

        final Path sourceDir = normalize(task.targetDir, null)
        if( sourceDir == null )
            return new LinkedHashSet<Path>(artifacts)

        final Set<Path> result = new LinkedHashSet<>()
        try {
            for( PublishDir directive : directives ) {
                if( directive == null || !directive.isEnabled() ) {
                    result.addAll(artifacts)
                    continue
                }

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
                        result.add(artifact)
                        continue
                    }

                    /*
                     * Re-running saveAs would execute user code twice and can
                     * diverge from Nextflow for stateful closures. Link-family
                     * publication is synchronous, so the real FilePublishEvent
                     * is already available at task completion and is the exact
                     * publication decision. Async modes remain conservative.
                     */
                    if( publicationObservedInline ) {
                        if( publishedSources.contains(artifact) )
                            result.add(artifact)
                    }
                    else {
                        result.add(artifact)
                    }
                }
            }
        }
        catch( Throwable e ) {
            log.warn "nf-gc could not classify publishDir artifacts for task ${task.name}; keeping outputs", e
            return new LinkedHashSet<Path>(artifacts)
        }
        finally {
            publishedSources.removeAll(artifacts)
        }

        return result
    }

    private static boolean publishesInline(PublishDir directive) {
        final PublishDir.Mode mode = directive.getMode()
        return mode == PublishDir.Mode.LINK || mode == PublishDir.Mode.SYMLINK || mode == PublishDir.Mode.RELLINK
    }

    private static Set<Path> outputPaths(TaskRun task) {
        final Set<Path> result = new LinkedHashSet<>()

        if( task.hasTypedInputsOutputs() ) {
            if( task.outputFiles != null )
                result.addAll(task.outputFiles)
            return result
        }

        final Map outputs = task.getOutputsByType(FileOutParam)
        for( Object value : outputs.values() ) {
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

        return result
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
            if( addProcessHold(path, process) )
                promoted.add(path)
        }
        releaseTaskHolds(task)
        return promoted
    }

    private List<DeletionResult> releaseTaskHoldsAndDelete(TaskRun task) {
        final Set<TaskProcessor> owners = releaseTaskHolds(task)
        final List<DeletionResult> results = new ArrayList<>()
        for( TaskProcessor owner : owners ) {
            if( dependencyClosed.contains(owner) )
                results.addAll(deleteReady(owner))
        }
        return results
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

    private Set<TaskProcessor> releaseProcessHolds(TaskProcessor holder) {
        final Set<Path> paths = heldArtifactsByProcess.remove(holder)
        if( paths == null || paths.isEmpty() )
            return Collections.emptySet()

        final Set<TaskProcessor> owners = newIdentityProcessSet()
        for( Path path : paths ) {
            final Set<TaskProcessor> holders = processHoldsByArtifact.get(path)
            if( holders != null ) {
                holders.remove(holder)
                if( holders.isEmpty() )
                    processHoldsByArtifact.remove(path)
            }

            final TaskProcessor owner = ownerByArtifact.get(path)
            if( owner != null )
                owners.add(owner)
        }
        return owners
    }

    private boolean isHeld(Path path) {
        final Set<TaskRun> taskHolders = taskHoldsByArtifact.get(path)
        if( taskHolders != null && !taskHolders.isEmpty() )
            return true
        final Set<TaskProcessor> processHolders = processHoldsByArtifact.get(path)
        return processHolders != null && !processHolders.isEmpty()
    }

    private List<DeletionResult> deleteReady(TaskProcessor process) {
        final Set<Path> paths = tracked.get(process)
        if( paths == null || paths.isEmpty() )
            return Collections.emptyList()

        final List<Path> ordered = new ArrayList<>(paths)
        Collections.sort(ordered, new Comparator<Path>() {
            @Override
            int compare(Path left, Path right) {
                return Integer.compare(right.nameCount, left.nameCount)
            }
        })

        final List<DeletionResult> results = new ArrayList<>()
        for( Path path : ordered ) {
            if( isHeld(path) )
                continue

            final DeletionResult result = deleteArtifact(process, path)
            results.add(result)

            /*
             * FAILED remains tracked so registry state never claims that a
             * file was reclaimed when it still exists. Missing and deleted
             * artifacts are terminal states for this run.
             */
            if( result.status != DeleteStatus.FAILED ) {
                paths.remove(path)
                ownerByArtifact.remove(path)
            }
        }

        if( paths.isEmpty() )
            tracked.remove(process)

        return results
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
