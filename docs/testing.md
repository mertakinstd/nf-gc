# Testing nf-gc

nf-gc uses two test layers: Gradle/Spock tests for deterministic plugin contracts and nf-test workflows for end-to-end Nextflow and filesystem behavior.

## Gradle tests

Run:

```bash
./gradlew clean test
```

These tests live under:

```text
src/test/groovy/
```

The current unit tests cover the observer factory and core artifact-registry behavior, including callback ordering, idempotence, pass-through holds, and deletion of already-missing artifacts.

## nf-test workflow tests

Install the plugin locally and run the synthetic workflow tests:

```bash
./gradlew installPlugin
nf-test test tests --verbose
```

The current suite contains 34 workflow cases under `tests/`. The regression matrix covers:

- dependency closure across fan-out, shared inputs, joins, aggregation, aliases, and zero-task processes;
- failed, aborted, retried, cached, missing, and deletion-failure cases;
- external inputs, staged pass-through outputs, symlinks, directories, globs, and optional outputs;
- `publishDir` modes and selection, `storeDir`, terminal outputs, and workflow outputs;
- an RNA-seq-like topology with shared index and multi-branch consumers.

The fixtures are intentionally small and use real filesystem assertions so retention and deletion behavior are tested against the same observable state that nf-gc modifies.

## Test expectations

Deletion is the risky operation. Tests that define the GC contract should therefore:

- make input, intermediate, published, and terminal artifact sets explicit;
- assert both reclaimed artifacts and artifacts that must be retained;
- exercise relevant callback-order and failure paths;
- prefer real filesystem assertions over mocks when deletion semantics are under test;
- add a regression case before changing an expectation that represents a safety boundary.

A change to expected deletion or retention behavior is a behavior change, not a snapshot update, and should be reviewed accordingly.
