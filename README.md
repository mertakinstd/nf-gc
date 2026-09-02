# nf-gc

## Summary

`nf-gc` is a Nextflow plugin for conservative, shallow garbage collection of workflow artifacts. It tracks successful non-cached task outputs owned by the Nextflow work directory and reclaims eligible intermediates after their producer becomes dependency-closed. When ownership or retention cannot be established safely, the artifact is kept.

## Get Started

For local development, bootstrap the pinned repository toolchain and install the plugin:

```bash
./scripts/bootstrap-dev.sh
source env.sh

./gradlew clean test
./gradlew installPlugin
```

A pipeline can then enable the locally installed plugin in `nextflow.config`:

```groovy
plugins {
    id 'nf-gc@0.1.0'
}
```

No nf-gc-specific pipeline configuration is required. The development environment uses Eclipse Temurin JDK 21, Nextflow 26.04.6, and nf-test 0.9.5. Docker is not required for the core plugin development loop.

## Examples

The repository includes synthetic workflows that exercise fan-out, shared inputs, joins, pass-through paths, publication behavior, failures, caching, and RNA-seq-like topology:

```bash
./test.sh
```

The regression tests use real filesystem state to verify both reclamation and conservative retention. For example, an eligible intermediate is reclaimed only after its producer and immediate downstream consumers have terminated; terminal or publication-sensitive outputs are retained.

## Scope and compatibility

- Minimum Nextflow version: `26.04.0`.
- Validated development/runtime version: `26.04.6`.
- Collection is shallow and limited to task outputs owned by the Nextflow work directory.
- Cached tasks, failed or unknown task states, terminal process outputs, and outputs outside task work ownership are kept.
- `publishDir` protection is artifact-level for pattern-selected outputs. For synchronous link-family publication, observed Nextflow publication events also resolve `saveAs` selection without re-running user closures; async `saveAs`, disabled publication, and otherwise ambiguous publication remain conservative.
- Any configured workflow output currently keeps intermediate task outputs for the run; artifact-level workflow-output provenance is not yet modelled.
- Staged external inputs are never acquired as owned artifacts. Re-emitted upstream artifacts are held until the relay dependency closes.
- Filesystem deletion does not follow symbolic links.
- Reclaimed artifacts are not guaranteed to remain available for a later `-resume` run.

The retention policy is intentionally conservative: uncertainty resolves to KEEP rather than DELETE.

## Contributing

- [Contribution guide](docs/contributing.md)
- [Reporting issues](docs/issues.md)
- [Testing](tests/testing.md)

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full license text.
