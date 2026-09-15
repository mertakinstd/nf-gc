# nf-gc

## Summary

`nf-gc` is a Nextflow plugin for conservative, shallow garbage collection of workflow artifacts. It tracks successful non-cached task outputs owned by the Nextflow work directory and reclaims eligible intermediates using either process-level or output-port-level dependency closure. When ownership or retention cannot be established safely, the artifact is kept.

## Get Started

Enable the plugin in your pipeline `nextflow.config`:

```groovy
plugins {
    id 'nf-gc@0.2.0'
}
```

Nextflow downloads the published plugin from the Nextflow Registry when the pipeline runs. No nf-gc-specific pipeline configuration is required. If the `nfGc` scope or `nfGc.gc_mode` is not configured, nf-gc uses `process` mode. This preserves the GC policy exposed by nf-gc 0.1.0; `artifact` mode is opt-in.

The policy can be selected explicitly from the plugin configuration:

```groovy
nfGc {
    gc_mode = 'process'
}
```

Supported modes are `process` and `artifact`. `process` is the backward-compatible default and preserves the original producer-wide closure semantics. `artifact` uses output-port-aware closure: an eligible artifact can be reclaimed after its producer process and every downstream consumer process reachable from that output port have terminated, without waiting for consumers of sibling output ports. Unknown values fail fast instead of silently changing GC semantics. At workflow startup nf-gc logs the resolved mode and whether the default was used.

`nf-gc` requires Nextflow `26.04.0` or newer.

## Examples

Run any Nextflow pipeline with the plugin enabled, either from `nextflow.config` as above or from the command line:

```bash
nextflow run main.nf -plugins nf-gc@0.2.0
```

With `gc_mode = 'process'`, eligible intermediate task outputs remain available until their producer process becomes dependency-closed. With `gc_mode = 'artifact'`, each artifact follows the dependency closure of its producer output port; sibling output ports no longer extend one another's lifetime. Artifact mode remains process-granular within each port and does not perform task-instance eager GC. Publication-sensitive, terminal, cached, failed, external, or otherwise uncertain artifacts are retained conservatively.

For repository development and the full regression suite:

```bash
./scripts/bootstrap-dev.sh
source env.sh
./test.sh
```

## Scope and compatibility

- Minimum Nextflow version: `26.04.0`.
- Validated development/runtime version: `26.04.6`.
- Collection is shallow and limited to task outputs owned by the Nextflow work directory.
- Cached tasks, failed or unknown task states, terminal process outputs, and outputs outside task work ownership are kept.
- `publishDir` protection is artifact-level for pattern-selected outputs. For synchronous link-family publication, observed Nextflow publication events also resolve `saveAs` selection without re-running user closures; async `saveAs`, disabled publication, and otherwise ambiguous publication remain conservative.
- Any configured workflow output currently keeps intermediate task outputs for the run; artifact-level workflow-output provenance is not yet modelled.
- In `artifact` mode, exact output-port provenance is resolved for legacy file outputs, including file members of tuple outputs. Typed/V2 outputs without an exact declaration-to-port mapping resolve conservatively to `UNKNOWN` and are kept.
- Staged external inputs are never acquired as owned artifacts. Re-emitted upstream artifacts are held until the relay dependency closes.
- Filesystem deletion does not follow symbolic links.
- Reclaimed artifacts are not guaranteed to remain available for a later `-resume` run.

The retention policy is intentionally conservative: uncertainty resolves to KEEP rather than DELETE.

## Validation

The behavior contract is covered by 46 functional regression cases against real Nextflow work directories and filesystem state. The plugin has also been integration-tested with `nf-core/rnaseq 3.26.0` using its official test profile, including STAR/Salmon, QC, MultiQC, hard-link publication, and nf-core-style `saveAs` retention gates.

## Contributing

- [Contribution guide](docs/contributing.md)
- [Reporting issues](docs/issues.md)
- [Testing](tests/testing.md)
- [Changelog](CHANGELOG.md)

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full license text.
