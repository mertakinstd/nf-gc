# Test specification

## Purpose

The functional suite is the behavioral contract for nf-gc. A passing case means that the described workflow situation is currently supported with the stated retention or reclamation semantics. The suite documents present behavior, not an aspirational implementation.

When a new behavior is proposed, its functional case is written first. The existing implementation is then run against that case: an expected failure establishes the missing capability; an unexpected pass is investigated before the behavior is considered supported. Implementation changes follow only after the expected contract is explicit.

## Validation model

`./test.sh` runs the complete validation stack from the repository root:

1. Gradle/Spock tests for deterministic plugin contracts;
2. plugin assembly and repo-local installation;
3. all 43 nf-test functional cases against real Nextflow work directories and filesystem state.

Cases F41-F43 derive from nf-core/rnaseq 3.26.0 publication semantics and pin both null and non-null `saveAs` branches under hard-link publication.

The functional cases below are the externally observable support matrix. Event traces establish lifecycle ordering and classification; filesystem assertions establish the actual retention or deletion result.

## Functional behavior contract

### Lifecycle integration

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F01 | One producer process runs multiple tasks and fans each result into two downstream processes. | Flow boundaries are observed once, each process is created/terminated once at process scope, and producer termination precedes both consumer terminations. | `runs a producer with two downstream consumers` |

### Representative RNA-seq topology

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F02 | A multi-sample RNA-seq-like graph combines a shared index, fan-out, two ALIGN output ports, joins, aggregation, publication, and an optional QC branch that is enabled. | The process graph matches the realized topology; each producer closes only after every immediate consumer terminates; internal index/read/BAM/count/quant/QC/merge artifacts are reclaimed after closure; `ALIGN_QC` is retained by `publishDir`; terminal `REPORT` is retained; published QC files survive. | `handles shared index, fan-out, join and aggregation` |
| F03 | The same RNA-seq-like workflow runs with the optional `EXTRA_QC` branch disabled. | The unrealized branch creates no process, edge, closure, or artifact state; the remaining graph closes and reclaims normally, while publication and terminal retention remain unchanged. | `handles an optional branch that is not instantiated` |

### Cached execution

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F04 | A process result is restored from a pre-existing `storeDir` cache instead of being produced by a new successful task. | The cached task is classified `CACHED`; its artifact is not acquired, tracked, or deleted; the cached file remains intact. | `keeps a task restored from storeDir cache` |

### Failure and deletion outcomes

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F05 | A task writes a declared output and then fails under `errorStrategy 'ignore'`. | Partial output from the failed task is not acquired for GC; the task resolves conservatively as `UNKNOWN` and the file remains. | `keeps outputs from a failed task` |
| F06 | Workflow termination aborts a still-running task after another task fails fast. | The aborted task is neither tracked nor reclaimed during shutdown. | `does not clean an aborted task during workflow termination` |
| F07 | A process fails its first attempt, retries successfully, and the successful output is consumed downstream. | The failed attempt is retained conservatively; only the successful attempt becomes an owned artifact and is reclaimed after normal dependency closure. | `keeps the failed retry attempt and deletes the successful attempt normally` |
| F08 | A downstream task removes an upstream owned artifact before nf-gc reaches deletion. | The absent artifact is reported as `ARTIFACT_MISSING`; absence is not treated as a deletion error and does not fail the workflow. | `reports a missing artifact without failing the workflow` |
| F09 | An owned directory cannot be removed because the filesystem rejects deletion. | The failure is reported as `ARTIFACT_DELETE_FAILED`; no successful-deletion event is emitted; the artifact remains and the workflow itself is not failed by GC. | `reports a filesystem deletion failure without failing the workflow` |

### Ownership and filesystem semantics

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F10 | An external file is staged with Nextflow's default staging and a process generates a new output from it. | The external source remains outside GC ownership; only the generated declared output is acquired and reclaimed. | `keeps an external input with default staging and deletes only generated output` |
| F11 | The same external-input pattern uses `stageInMode 'copy'`. | Copy staging does not transfer ownership of the external source; only the generated output is acquired and reclaimed. | `keeps an external input when stageInMode is copy` |
| F12 | A process re-emits an external staged input with `includeInputs: true`. | The relay does not acquire the staged input as its own artifact; the path is held rather than tracked/deleted by the relay, and the external source survives. | `does not acquire ownership when an external staged input is re-emitted` |
| F13 | A generated upstream artifact is passed through a default-staged relay and consumed later. | The relay holds the upstream artifact; producer closure alone is insufficient for deletion; reclamation occurs only after the relay dependency also closes; the relay never becomes the owner. | `holds an upstream artifact through a default-staged pass-through relay` |
| F14 | The same generated pass-through uses `stageInMode 'copy'` at the relay. | The upstream artifact is conservatively held through relay closure and reclaimed only afterwards; the relay copy is not acquired as a new owned artifact. | `conservatively holds an upstream artifact through a copy-staged pass-through relay` |
| F15 | A declared generated output is a symbolic link to an external file. | The owned symlink is reclaimed without following or deleting its external target. | `deletes a generated symlink without following its external target` |
| F16 | A declared directory artifact contains both generated content and a symlink to an external file. | Recursive reclamation removes the owned directory tree without following the embedded symlink; the external target survives. | `recursive directory cleanup does not follow an external symlink` |
| F17 | One glob declaration realizes multiple output files. | Every realized matching file becomes an owned artifact and is reclaimed independently; no synthetic missing artifact is invented. | `tracks every realized file in a glob output` |
| F18 | A process declares one required output and one optional output that is not realized. | Only the realized required path enters the artifact lifecycle; the absent optional declaration does not create a tracked or missing artifact. | `does not invent a missing artifact for an absent optional output` |
| F19 | A task work directory contains two declared outputs, undeclared tool/scratch files, and Nextflow `.command.*` infrastructure. | Only the two declared runtime outputs are acquired and reclaimed; undeclared side files and Nextflow task infrastructure remain untouched. | `reclaims only declared outputs from a task with undeclared side files` |
| F20 | A tool log is explicitly declared as a process output alongside another file. | Artifact ownership is determined by Nextflow output provenance rather than extension or purpose; both declared files are acquired and reclaimed. | `reclaims a log when the log is a declared process output` |
| F21 | A glob declaration realizes three BAM outputs while the same task creates an undeclared log. | All three realized BAMs are acquired/reclaimed; the non-matching undeclared log is not acquired and remains in the work directory. | `tracks all realized glob outputs without acquiring an undeclared side file` |
| F22 | A tuple output combines scalar metadata with two path members. | Only filesystem path members become GC artifacts; tuple metadata remains outside the artifact model. | `tracks only path members of a tuple output` |
| F23 | A process declares a nested directory as its output while leaving unrelated files in the same task work directory. | The declared directory is treated as one owned artifact and reclaimed recursively; unrelated task files and Nextflow infrastructure are not cleaned. | `reclaims a declared directory without cleaning unrelated task files` |
| F24 | One task contains a staged external input plus multiple generated declared outputs. | The staged input remains unowned and its external source survives; each generated output is independently acquired and reclaimed. | `keeps a staged external input while reclaiming multiple generated outputs` |

### Publication and retention surfaces

Publication retention follows the artifact selected for publication. A `publishDir` therefore protects matching outputs without shielding unpublished siblings from their normal GC lifecycle. Ambiguous publication configuration remains conservative.

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F25 | `publishDir` selects an artifact using `mode: 'copy'`. | The selected work artifact is protected from GC and the published copy remains valid. | `preserves an artifact published with copy mode` |
| F26 | `publishDir` selects an artifact using `mode: 'copyNoFollow'`. | The selected work artifact is protected from GC without changing publication-mode semantics. | `preserves an artifact published with copyNoFollow mode` |
| F27 | `publishDir` selects an artifact using hard-link mode `link`. | The selected source artifact is protected so the published hard link is not invalidated by GC. | `preserves an artifact published with hard-link mode` |
| F28 | `publishDir` selects an artifact using absolute symlink mode `symlink`. | The selected source artifact is protected and the published symlink remains usable. | `preserves an artifact published with absolute-symlink mode` |
| F29 | `publishDir` selects an artifact using relative symlink mode `rellink`. | The selected source artifact is protected and the published relative symlink remains usable. | `preserves an artifact published with relative-symlink mode` |
| F30 | `publishDir` selects an artifact using `mode: 'move'`. | nf-gc does not acquire the selected artifact while Nextflow transfers ownership to the publish destination. | `does not acquire an artifact published with move mode` |
| F31 | A declared output is both selected by `publishDir` and consumed downstream. | Publication protection takes precedence for that artifact: it is not acquired for GC and the published result survives. | `preserves an output selected by publishDir while it is consumed downstream` |
| F32 | A task produces a QC/log artifact and a BAM artifact; the `publishDir` pattern publishes only the QC/log while the BAM is consumed downstream. | Publication protects only the selected QC/log artifact. The QC/log remains in work and in the publish directory; the unpublished BAM is acquired normally and reclaimed after its downstream dependency closes. | `reclaims an unpublished output while preserving a published sibling` |
| F33 | A task has multiple `publishDir` directives selecting different outputs. | Publication selection is the union of the directives; each selected artifact is protected and both published destinations survive. | `preserves outputs selected by multiple publishDir directives` |
| F34 | A task declares `publishDir` with `enabled: false`. | **Current contract:** configured `publishDir` is still treated conservatively as `PUBLISH_DIR`; no file is published, but the task output is retained rather than reclaimed. | `freezes the runtime semantics of publishDir enabled false conservatively` |
| F35 | A process writes through `storeDir`, so its target directory differs from normal task work ownership. | The output is classified `TARGET_DIR`, is not acquired/reclaimed by nf-gc, and the stored artifact survives. | `keeps storeDir outputs outside task work ownership` |
| F36 | A process has no downstream consumers and no `publishDir`. | Terminal-process outputs are classified `TERMINAL` and retained rather than reclaimed. | `keeps a terminal process without publishDir` |

### Process topology and closure

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F37 | One process produces two output ports consumed by a fast and a slow downstream process. | Closure is process-level: neither source artifact is reclaimed when only the fast branch finishes; both are reclaimed only after all immediate consumers terminate and the producer closes. | `waits for the slow consumer before deleting either output port` |
| F38 | The same module implementation is instantiated under different aliases/scopes in nested workflows. | Scoped process identities remain distinct; graph edges do not cross aliases, and each instance's artifact is reclaimed under its own dependency chain. | `keeps aliased module instances distinct across nested workflow scope` |
| F39 | A process graph is instantiated over an empty channel and therefore runs zero tasks. | The process edge still exists; each process dependency closes exactly once; no artifact is invented, tracked, deleted, or reported as a deletion failure. | `closes a zero-task process graph without inventing artifacts` |

### Workflow outputs

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F40 | A non-terminal intermediate is explicitly exposed through the Nextflow workflow-output publication surface while also feeding a downstream consumer. | The artifact is classified `WORKFLOW_OUTPUT`, is not reclaimed from work, and the workflow-output copy is published successfully. | `keeps a non-terminal artifact selected as a workflow output` |

### nf-core/rnaseq publication semantics

These cases mirror publication idioms used by nf-core/rnaseq 3.26.0: pipeline-wide filename filtering through `saveAs`, parameter-gated STAR intermediates, and hard-link publication. For link-family modes, nf-gc uses Nextflow's actual synchronous file-publication event instead of evaluating the `saveAs` closure a second time. Async publication with `saveAs` remains conservative.

| ID | Supported situation | Expected nf-gc behavior | Evidence |
| --- | --- | --- | --- |
| F41 | An nf-core-style default `publishDir` has no pattern and uses `saveAs` to reject only `versions.yml` while publishing a sibling result. | The published result is protected; `versions.yml`, for which Nextflow emits no publication event, is not publication-protected and may be reclaimed after process dependency closure. | `reclaims a file rejected by an nf-core-style filename saveAs filter` |
| F42 | A STAR-like task emits `sample.bam`, `Log.final.out`, and `SJ.out.tab`; logs are selected by pattern, while the matching BAM is gated by `saveAs` with `save_align_intermeds = false`. | STAR logs remain published/protected; the BAM is not published, enters the normal GC lifecycle, and is reclaimed after its downstream consumer closes. | `reclaims an nf-core-style gated BAM when save_align_intermeds is false` |
| F43 | The same STAR-like publication surface runs with `save_align_intermeds = true`. | The BAM receives a non-null `saveAs` result, is published alongside the logs, and is never acquired or reclaimed by nf-gc. | `preserves an nf-core-style gated BAM when save_align_intermeds is true` |

## Current boundaries encoded by the suite

These are intentional descriptions of the current contract, not claims about the final design:

- dependency closure and reclamation are process-level, not per-consumer-task or per-output-port;
- `publishDir` retention is artifact-level for pattern selection; link-family `saveAs` decisions use Nextflow's observed publication events, while async `saveAs` publication and disabled publication remain conservatively retained;
- workflow-output presence is handled conservatively; artifact-level workflow-output provenance is not yet a GC decision surface;
- only realized Nextflow task outputs under established work ownership can be reclaimed; nf-gc is not a general work-directory cleaner;
- uncertainty, failed/cached execution, external ownership, and unsupported target ownership resolve to retention.

A future capability changes this contract only after its expected behavior is represented by a functional test. The initial run may fail; that failure is the evidence that implementation work is required.

## Running the contract

The one-time pinned toolchain setup remains separate:

```bash
./scripts/bootstrap-dev.sh
```

Run the complete validation from the repository root:

```bash
./test.sh
```

A change is acceptable only when the intended new/changed functional contract and all unaffected existing contracts pass together.
