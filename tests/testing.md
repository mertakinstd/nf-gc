# Testing nf-gc

The functional suite is the behavioral contract for nf-gc. It is intentionally
organized around externally observable Nextflow lifecycle semantics rather than
around the current implementation classes.

A behavior change should be expressed in the functional contract before the
implementation is changed. A failing contract is useful evidence that the
runtime behavior does not yet implement the intended semantics; unrelated
contracts must remain stable.

## Test layers

### Semantic contract

`tests/semantics/` owns normal artifact-lifetime behavior.

`tests/semantics/model.nf` is the shared model workflow for both GC policies. A
single workflow execution contains the nf-gc-relevant lifetime relationships:

- one producer task to one consumer task;
- independent sibling output ports;
- one artifact fanning out to multiple consumers;
- pass-through / re-emitted artifacts;
- declared outputs with no downstream demand;
- terminal declared outputs;
- one singleton artifact consumed by multiple task instances;
- delayed demand created by `collect`;
- keyed delayed demand created by `join`;
- runtime conditional routing where each artifact reaches only one concrete branch;
- partial publication where one sibling is published and another is not.

The model is intentionally domain-neutral. It represents Nextflow semantics,
not a synthetic scientific pipeline.

The same model is evaluated separately under:

- `tests/semantics/process/` for the concrete-Path process clock;
- `tests/semantics/artifact/` for the concrete task/runtime artifact clock.

A semantic test should state one GC proposition. Structural assertions may
establish the prerequisite lifecycle events, but the test should have one
primary KEEP/DELETE/timing claim. Semantic tests are the executable
specification of how nf-gc should behave; they must not exist merely to prove
that a rejected design, an uninstantiated branch, or a historical implementation
detail is absent. A negative assertion is appropriate only when it expresses a
real GC invariant, such as keeping a live artifact until its final legal demand
has completed.

#### Semantic contract catalogue

Each semantic test below has one product-level purpose. The two modes share one
ownership/retention model and differ only in the liveness evidence required before
reclaiming an eligible Path.

| Mode | Test | GC proposition |
| --- | --- | --- |
| artifact | `reclaims one-to-one artifacts after their concrete consumer task completes` | A concrete artifact is reclaimed after its own consumer task completes, without waiting for sibling task instances or process termination. |
| artifact | `reclaims an output port independently of a slow sibling port` | An artifact on one output port is not kept alive by an unrelated sibling output port. |
| artifact | `reclaims a fan-out artifact only after its concrete consumers complete` | A fan-out artifact stays live until all of its concrete consumers finish, then is reclaimed independently of other artifact instances. |
| artifact | `reclaims a pass-through source after its concrete downstream lineage completes` | Re-emission does not extend an artifact beyond the lifetime of its own concrete downstream lineage. |
| artifact | `reclaims an unused declared output at producer-task granularity` | A declared output with no legal downstream demand is reclaimed as soon as its producer task makes that fact actionable. |
| artifact | `reclaims a zero-consumer terminal output at producer-task granularity` | A non-retained terminal artifact with no legal consumer is reclaimed per producer task rather than waiting for the producer process. |
| artifact | `does not reclaim a shared singleton before its last concrete consumer completes` | A shared artifact remains live while any concrete consumer task still requires it. |
| artifact | `does not reclaim collected artifacts before the aggregate consumer completes` | Buffered/aggregated demand prevents premature reclamation before the aggregate consumer completes. |
| artifact | `does not reclaim a joined artifact before its keyed consumer completes` | Delayed keyed demand prevents premature reclamation before the corresponding joined consumer completes. |
| artifact | `reclaims a conditionally routed artifact after only its concrete branch completes` | Runtime routing follows the concrete branch taken by an artifact; untaken potential branches do not extend its lifetime. |
| artifact | `retains the published artifact without pinning its unpublished sibling` | Publication protects only the concrete published artifact; an unpublished sibling follows normal artifact GC. |
| artifact | `retains the concrete workflow output without pinning an unrelated intermediate` | Exact workflow-output retention is mode-independent and does not globally pin unrelated intermediates. |
| artifact | `retains an upstream concrete artifact when its pass-through lineage is a workflow output` | Workflow-output retention follows exact same-Path pass-through lineage back to the owned upstream artifact. |
| artifact | `falls back globally when workflow-output provenance is derived and untrusted` | A derived/unmatched workflow-output channel makes artifact reclamation conservative for the run rather than guessing provenance. |
| artifact | `retains overlapping declared Paths as one conservative physical tree` | Ancestor/descendant outputs are retained together so deleting one declaration cannot corrupt an active consumer, publication, or workflow output; unrelated siblings remain independently collectible. |
| artifact | `reclaims conditionally unpublished hard-link intermediates at their concrete last-use boundaries` | A hard-link `publishDir` whose `saveAs` rejects an intermediate does not create an artificial publication hold; direct and operator-backed intermediates are reclaimed during the run once their real demand closes, while published siblings remain retained. |
| artifact | `does not treat a topic file as terminal before its topic demand completes` | A `topic:` output is not reclaimed merely because its normal DAG edge is a leaf; topic demand must complete before reclamation. |
| process | `reclaims one-to-one artifacts only after the consumer process terminates` | Process mode uses consumer-process closure rather than individual consumer-task completion. |
| process | `reclaims sibling output ports at their own consumer-process boundaries` | Process mode keeps concrete Path identity and output-port provenance, but waits for the producer and that Path's downstream consumer processes rather than individual consumer tasks. |
| process | `waits for every fan-out consumer process before reclaiming producer artifacts` | Process mode retains a fan-out producer artifact until every downstream consumer process closes. |
| process | `holds pass-through sources until the downstream relay process terminates` | Exact pass-through lineage keeps the upstream Path identity, while process mode advances it at the relay's process-level route boundary. |
| process | `reclaims an unused output at producer-process termination without waiting for a used sibling` | A terminal Path has no consumer-process dependency, so process mode reclaims it after its producer process terminates even when another output port remains live. |
| process | `reclaims terminal-process outputs at coarse process closure` | Ordinary terminal Paths are reclaimed after their producer process terminates; topic-backed or otherwise process-invisible demand still requires the appropriate route/global seal. |
| process | `holds a shared singleton until the consumer process terminates` | A shared artifact remains retained until its consumer process closes in process mode. |
| process | `holds collected inputs until the aggregate consumer process terminates` | Aggregated inputs remain retained until the aggregate consumer process closes. |
| process | `holds joined inputs until the keyed consumer process terminates` | Joined inputs remain retained until the keyed consumer process closes. |
| process | `keeps conditionally routed artifacts coupled until every potential consumer process terminates` | Process mode follows the coarse process graph, so potential routed consumer processes participate in closure. |
| process | `retains the published artifact while reclaiming its sibling after its consumer process terminates` | Publication protects the selected artifact while its unpublished sibling follows process-level GC. |
| process | `reclaims the same hard-link intermediates only after process-level closure` | The publication-aware fixture has the same eventual reclaim set as artifact mode, but process mode waits for the corresponding process-level closure. |
| process | `retains the concrete workflow output without pinning an unrelated intermediate` | Exact workflow-output retention is shared with artifact mode; unrelated eligible intermediates follow process-level closure. |
| process | `retains an upstream concrete artifact when its pass-through lineage is a workflow output` | Exact workflow-output retention propagates through same-Path pass-through lineage in process mode as well. |
| process | `falls back globally when workflow-output provenance is derived and untrusted` | Derived/unmatched workflow-output provenance is conservative in both modes. |
| process | `waits for the global flow seal when topic demand is outside the process projection` | Process mode does not treat topic-backed demand as closed merely because no ordinary process consumer is visible. |

Field-derived lifetime cases also use small dedicated fixtures when adding them to
the shared model would make every semantic assertion pay for unrelated mechanics.
`tests/semantics/peak_disk.nf` models the publication-aware shape that matters for
large intermediates: conditional `saveAs`, hard-link publication, direct demand,
and operator-backed demand. `tests/semantics/topic_lifetime.nf` covers the
Nextflow `topic:` demand surface that is not represented by an ordinary producer
DAG edge.

Workflow-output cases use small dedicated fixtures because declaring a workflow
output changes the run-global Nextflow output surface. Keeping those declarations
outside the shared model prevents them from changing unrelated semantic cases.
The pass-through and untrusted-provenance fixtures separately cover the two
run-level retention boundaries that cannot coexist with the direct-specificity
contract in a single execution. `tests/semantics/overlapping_outputs.nf` covers
filesystem containment: ancestor/descendant declared outputs are one physical
tree for deletion safety even though Nextflow exposes them as separate output
Paths.

### Regression contract

`tests/regression/` owns exceptional execution mechanics and narrow Nextflow
integration regressions rather than the normal liveness policy. This includes:

- failed, aborted, retried, cached, missing, and deletion-failure tasks;
- external ownership and staging modes, including declared descendants of staged directories;
- symlink and directory deletion safety, including task-created symlink ancestors, aliases between declared output trees, and retained symlink outputs backed by upstream owned artifacts;
- glob, tuple, optional, and undeclared side-file ownership;
- concrete `publishDir` modes, `saveAs`, and `storeDir`;
- scoped module aliases and zero-task graphs;
- retained exact pass-through outputs whose physical storage is still owned by an upstream task.

A normal lifecycle topology should not be duplicated in regression merely to
restate the process or artifact policy.

### Deterministic unit tests

`src/test/` covers implementation-local data structures and safety rules. Unit
tests may inspect internal classes. Functional semantic tests must not use an
internal registry/state class as their oracle.

## Observable lifecycle trace

Functional tests enable `NF_GC_TEST_TRACE` through `tests/nextflow.config`.
External acceptance or field runs must do the same through Nextflow's config
`env` scope, for example `env.NF_GC_TEST_TRACE = 'true'`. The observer reads
`Session.config.env` intentionally; a host-shell `export NF_GC_TEST_TRACE=true`
alone does not enable the trace. This is test-only observability and is not part
of the public nf-gc config API.

The observer exposes stable lifecycle boundaries including:

```text
FLOW_CREATE
FLOW_BEGIN
PROCESS_CREATE
TASK_PENDING
TASK_START
TASK_COMPLETE
TASK_CACHED
FILE_PUBLISH
WORKFLOW_OUTPUT
PROCESS_TERMINATE
DEPENDENCY_CLOSED
ARTIFACT_TRACKED
ARTIFACT_ROUTE
OUTPUT_PORT_CLOSED
RUNTIME_ROUTE_SEALED
ARTIFACT_HOLD
ARTIFACT_KEEP
ARTIFACT_DELETED
FLOW_COMPLETE
```

Task lifecycle records include the Nextflow task identity, process name, task
name/tag, and work directory. `TASK_COMPLETE` is recorded before any nf-gc
reclamation triggered by that completion so tests can make an unambiguous
ordering assertion.

The route events are diagnostic evidence for the clock, not additional GC
policy. Their tab-separated payloads are:

```text
ARTIFACT_ROUTE        <path> <port> <route-kind> <demand-kind> <consumers> <global-seal>
OUTPUT_PORT_CLOSED    <port> <route-kind> <consumers> <global-seal>
RUNTIME_ROUTE_SEALED  <port> <route-kind> <consumers> <global-seal>
```

`<port>` is the producer process plus the identity of the exact Nextflow channel
object (for example `STAR_ALIGN@4ab12cd`). It is intentionally a run-local
correlation key, not a stable public identifier. `route-kind` describes the DAG
shape (`DIRECT`, `TERMINAL`, or `FALLBACK`); `demand-kind` is the stricter
artifact-clock classification and can fall back even when the graph route itself
is direct. Consumer names are comma-separated and may be empty. `ARTIFACT_ROUTE`
is emitted only when exact Path-to-port provenance exists. The same port key
appearing later in `OUTPUT_PORT_CLOSED` or `RUNTIME_ROUTE_SEALED` lets a field
test distinguish a concrete task-use delay from a process or operator-route
sealing delay without reimplementing the Nextflow graph in the test harness.

`ARTIFACT_HOLD` records an explicit coarse process/global fallback hold. Exact
pass-through provenance can instead extend the same artifact's downstream
liveness directly, so absence of `ARTIFACT_HOLD` is not evidence that an
artifact is unprotected. Safety tests should assert ownership and lifecycle
ordering at the relevant deletion boundary rather than require this event for
exact lineage.

`FILE_PUBLISH` records the concrete source path supplied by Nextflow. Tests can
therefore distinguish publication of one artifact from retention of an
unrelated sibling without reproducing `publishDir` selection logic themselves.

For async `publishDir saveAs`, nf-gc deliberately does not re-run the user closure.
A candidate that has not yet emitted `FILE_PUBLISH` remains held during the run.
On successful `FLOW_COMPLETE`, Nextflow has already drained the publication pool.
When that directive also has `failOnError` enabled, absence of `FILE_PUBLISH` is
exact negative evidence and the hold may be released safely. With publish failures
configured as non-fatal, absence of the event remains ambiguous and is retained.
Disabled publish directives are exact non-publication and do not create a retention
hold.

## Semantic rules for test authors

Functional tests should observe Nextflow events and filesystem state, not infer
correctness from the current nf-gc implementation.

In particular:

- artifact-mode eager reclamation may be required only when the fixture proves
  that the concrete artifact has no remaining legal demand; tests that distinguish
  task-level reclamation derive the first eligible artifact from the observed task
  lifecycle rather than assuming sample-label or scheduler order, then require
  deletion after that artifact becomes eligible and before a later sibling task
  becomes eligible. `PROCESS_TERMINATE` is not an artifact-mode deadline because
  Nextflow may report processor termination before the final task-completion event;
- shared singleton inputs, `collect`, `join`, and other delayed-demand shapes
  must forbid premature deletion, but must not require eager deletion before a
  safe demand-sealing boundary is observable;
- conditional routing must follow the concrete artifact path rather than making
  one routed item wait for consumer branches that never receive that item;
- unknown ownership/provenance remains conservative unless a separate contract
  explicitly establishes a safe reclaim rule;
- overlapping ancestor/descendant output Paths are not independent deletion
  units; the overlapping physical tree must remain conservative while unrelated
  siblings may still follow their normal GC contract;
- a declared output below a staged input directory belongs to that staged source
  tree and must not be acquired as a new owned artifact from lexical work-path
  membership alone;
- a declared output reached through a task-created symlink ancestor, or a declared
  symlink that aliases another declared output tree, is not an independent deletion
  unit; when such an alias resolves into an upstream nf-gc-owned artifact tree, that
  backing artifact is retained conservatively instead of inferring alias lineage;
- undeclared files in a task work directory are not nf-gc artifacts merely
  because they exist;
- published, external, cached, failed, or otherwise protected artifacts remain
  governed by their dedicated retention contracts.

Process mode is a timing policy over the same concrete-Path detection model, not
a separate ownership implementation. Process semantic tests must verify process
boundaries without reintroducing producer-wide coupling between sibling Paths;
artifact-mode tests must still prove earlier concrete last-use reclamation.

## Running the contract

Install the pinned development toolchain once:

```bash
./scripts/bootstrap-dev.sh
```

Then run the complete validation from the repository root:

```bash
source scripts/env.sh
./scripts/test.sh
```

A change is acceptable only when the intended semantic contract and all
unaffected regression and unit contracts agree with the implementation.

## Third-party acceptance

Pull requests that change runtime, build, test, or acceptance surfaces also run a
pinned `nf-core/rnaseq` 3.26.0 acceptance workload against Nextflow 26.04.6. The
acceptance job executes three representative combinations in one runner so the
nf-core checkout and Docker cache are shared:

- `process + link`, as the conservative lifetime baseline for the production-relevant publish mode;
- `artifact + link`, which must converge on the same eventual reclaim set while reclaiming concrete intermediates earlier;
- `artifact + copy`, which exercises asynchronous publication reconciliation without making delayed publication a required limitation.

The gate verifies 234 successful tasks, the frozen 171-artifact eventual reclaim
set, parity of the concrete deletion multiset across the three runs, intact
published mark-duplicate BAMs and MultiQC output, non-zero mid-run reclamation in
`process + link`, and a strict mid-run timing advantage in `artifact + link`. The
artifact-link run must also reclaim the STAR genomic BAMs, STAR transcriptome BAMs,
and SAMtools sorted intermediate BAMs before the final task completes. This is a
third-party acceptance layer, not the semantic specification; the small
deterministic fixtures remain the primary behavioral contract.

For timing investigations, prefer a targeted `artifact + link` rerun with the
test trace enabled rather than inferring causality from wall-clock seconds.
Correlate `ARTIFACT_ROUTE` with `TASK_*`, `RUNTIME_ROUTE_SEALED`,
`OUTPUT_PORT_CLOSED`, and `ARTIFACT_DELETED` to identify the exact gate that made
a Path reclaimable.
