# Changelog

All notable changes to `nf-gc` are documented in this file.

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
