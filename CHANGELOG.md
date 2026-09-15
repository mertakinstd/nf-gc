# Changelog

All notable changes to `nf-gc` are documented in this file.

## 0.2.0 - 2026-09-15

### Added

- Public `nfGc.gc_mode` policy selection with `process` as the backward-compatible default and `artifact` as the second accepted policy value.
- A single workflow-start INFO log reporting the resolved GC mode and whether the default `process` policy was selected because no mode was configured.
- Formal Nextflow `ConfigScope` registration for `nfGc.gc_mode`, eliminating unrecognized-option warnings while preserving runtime validation and defaults.
- Output-port-aware `artifact` reclamation that closes each producer output port against only its own downstream consumer processes while retaining process-level safety within the port.
- Deterministic output-port graph/closure tests plus functional coverage for independent sibling ports, same-port fan-out, and RNA-seq-like multi-output topology.

## 0.1.0 - 2026-09-02

Initial public release.

### Added

- Conservative in-run garbage collection for eligible Nextflow task outputs.
- Process-level dependency closure with artifact-level ownership and retention classification.
- Artifact-level `publishDir` protection, including nf-core-style synchronous link-family `saveAs` handling without re-running user closures.
- Conservative retention for cached, failed, terminal, workflow-output, target-directory, disabled-publication, async-`saveAs`, and otherwise ambiguous artifacts.
- Staged-input and pass-through protection so external or re-emitted upstream artifacts are not reclaimed prematurely.
- Recursive directory deletion that does not follow symbolic links.
- A 43-case functional regression suite covering lifecycle, ownership, publication, topology, workflow-output safety, and RNA-seq-like workflows.
- Integration validation against `nf-core/rnaseq 3.26.0` using its official test profile.
