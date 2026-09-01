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
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.Comparator
import java.util.IdentityHashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.params.FileOutParam

/**
 * Tracks Nextflow-owned task outputs that can be reclaimed once their producer
 * process becomes dependency-closed.
 *
 * The registry intentionally has a narrow first policy:
 *   - only successful, non-cached task outputs are considered;
 *   - only outputs owned by the task work directory are considered;
 *   - tasks with any publishDir configuration are kept;
 *   - terminal-process outputs are kept until workflow-output semantics are
 *     modelled explicitly;
 *   - staged inputs, storeDir outputs and unknown cases are kept.
 */
@Slf4j
@CompileStatic
final class GcArtifactRegistry {

    static final String KEEP_PUBLISH_DIR = 'PUBLISH_DIR'
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
        final List<DeletionResult> deletions
        final String keepReason

        Update(TaskProcessor process, Collection<Path> tracked, Collection<DeletionResult> deletions, String keepReason=null) {
            this.process = process
            this.tracked = Collections.unmodifiableList(new ArrayList<Path>(tracked))
            this.deletions = Collections.unmodifiableList(new ArrayList<DeletionResult>(deletions))
            this.keepReason = keepReason
        }
    }

    @CompileStatic
    static final class DeletionResult {
        final Path path
        final DeleteStatus status
        final String error

        DeletionResult(Path path, DeleteStatus status, String error=null) {
            this.path = path
            this.status = status
            this.error = error
        }
    }

    private final GcProcessGraph graph
    private final Map<TaskProcessor, Set<Path>> tracked = new IdentityHashMap<>()
    private final Set<TaskProcessor> dependencyClosed = newIdentityProcessSet()

    GcArtifactRegistry(GcProcessGraph graph) {
        if( graph == null )
            throw new IllegalArgumentException('Process graph must not be null')
        this.graph = graph
    }

    synchronized Update onTaskComplete(TaskRun task) {
        if( task == null || task.processor == null || !graph.contains(task.processor) )
            return new Update(task?.processor, Collections.<Path>emptyList(), Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)

        final TaskProcessor process = task.processor

        if( task.cached )
            return new Update(process, Collections.<Path>emptyList(), Collections.<DeletionResult>emptyList(), KEEP_CACHED)

        if( !task.isSuccess() )
            return new Update(process, Collections.<Path>emptyList(), Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)

        if( hasPublishDir(task) )
            return new Update(process, Collections.<Path>emptyList(), Collections.<DeletionResult>emptyList(), KEEP_PUBLISH_DIR)

        if( graph.consumersOf(process).isEmpty() )
            return new Update(process, Collections.<Path>emptyList(), Collections.<DeletionResult>emptyList(), KEEP_TERMINAL)

        final Path workDir = normalize(task.workDir, null)
        final Path targetDir = normalize(task.targetDir, null)
        if( workDir == null || targetDir == null || !targetDir.equals(workDir) )
            return new Update(process, Collections.<Path>emptyList(), Collections.<DeletionResult>emptyList(), KEEP_TARGET_DIR)

        final Set<Path> outputs
        try {
            outputs = outputPaths(task)
        }
        catch( Throwable e ) {
            log.warn "nf-gc could not resolve output artifacts for task ${task.name}; keeping outputs", e
            return new Update(process, Collections.<Path>emptyList(), Collections.<DeletionResult>emptyList(), KEEP_UNKNOWN)
        }

        final Set<Path> stagedInputs = stagedInputPaths(task, workDir)
        Set<Path> processArtifacts = tracked.get(process)
        if( processArtifacts == null ) {
            processArtifacts = new LinkedHashSet<Path>()
            tracked.put(process, processArtifacts)
        }
        final List<Path> newlyTracked = new ArrayList<>()

        for( Path raw : outputs ) {
            final Path artifact = normalize(raw, workDir)
            if( artifact == null || artifact == workDir || !artifact.startsWith(workDir) )
                continue
            if( stagedInputs.contains(artifact) )
                continue
            if( processArtifacts.add(artifact) )
                newlyTracked.add(artifact)
        }

        final List<DeletionResult> deletions = dependencyClosed.contains(process)
            ? deleteTracked(process)
            : Collections.<DeletionResult>emptyList()

        return new Update(process, newlyTracked, deletions)
    }

    synchronized List<DeletionResult> onDependencyClosed(TaskProcessor process) {
        if( process == null || !graph.contains(process) )
            return Collections.emptyList()

        dependencyClosed.add(process)
        return Collections.unmodifiableList(deleteTracked(process))
    }

    private boolean hasPublishDir(TaskRun task) {
        try {
            return !!task.config?.getPublishDir()
        }
        catch( Throwable e ) {
            log.warn "nf-gc could not resolve publishDir for task ${task.name}; keeping outputs", e
            return true
        }
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

    private static Set<Path> stagedInputPaths(TaskRun task, Path workDir) {
        final Set<Path> result = new LinkedHashSet<>()
        final Map<String,Path> inputs = task.inputFilesMap
        if( inputs == null )
            return result

        for( String stagedName : inputs.keySet() ) {
            if( stagedName == null )
                continue
            result.add(workDir.resolve(stagedName).toAbsolutePath().normalize())
        }
        return result
    }

    private List<DeletionResult> deleteTracked(TaskProcessor process) {
        final Set<Path> paths = tracked.remove(process)
        if( paths == null || paths.isEmpty() )
            return Collections.emptyList()

        final List<Path> ordered = new ArrayList<>(paths)
        Collections.sort(ordered, new Comparator<Path>() {
            @Override
            int compare(Path left, Path right) {
                return Integer.compare(right.nameCount, left.nameCount)
            }
        })

        final List<DeletionResult> results = new ArrayList<>(ordered.size())
        for( Path path : ordered )
            results.add(deleteArtifact(path))
        return results
    }

    private static DeletionResult deleteArtifact(Path path) {
        if( !Files.exists(path, LinkOption.NOFOLLOW_LINKS) )
            return new DeletionResult(path, DeleteStatus.MISSING)

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
            return new DeletionResult(path, DeleteStatus.DELETED)
        }
        catch( IOException e ) {
            return new DeletionResult(path, DeleteStatus.FAILED, e.message)
        }
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
}
