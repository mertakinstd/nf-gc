# Contributing to nf-gc

Contributions should keep `nf-gc` small, conservative, and explicit about filesystem behavior. Changes that affect artifact retention or deletion must be testable and should avoid depending on unstable Nextflow internals when a supported plugin extension point is available.

## Set up the development environment

The repository uses a repo-local, pinned toolchain so development does not depend on whichever Java, Nextflow, or nf-test version happens to be installed on the host.

```bash
git clone https://github.com/mertakinstd/nf-gc.git
cd nf-gc

./scripts/bootstrap-dev.sh
source env.sh
```

The bootstrap installs the following under `.tools/`:

- Eclipse Temurin JDK `21.0.12+8`
- Nextflow `26.04.6`
- nf-test `0.9.5`

Downloads are checksum-verified. `env.sh` sets `JAVA_HOME`, prepends the repo-local tools to `PATH`, pins `NXF_VER=26.04.6`, and uses the repo-local `.nxf/` directory as `NXF_HOME`.

The bootstrap does not install system packages and never uses `sudo`, `apt`, `dnf`, Homebrew, or another package manager. Linux and macOS are supported on x86_64 and arm64/aarch64.

Docker is optional. It is not required for the core Gradle, synthetic Nextflow, or nf-test development loop. External integration validation, such as an nf-core pipeline run, may require Docker or another container runtime.

## Build and test

Run the core checks from the repository root:

```bash
./test.sh
```

See [the test specification](../tests/testing.md) for the regression model and acceptance criteria.

## Repository layout

```text
src/main/groovy/   Plugin implementation
src/test/groovy/   Gradle/Spock tests
tests/             Synthetic workflows, nf-test cases, and test specification
scripts/           Repository development utilities
docs/              Contributor and issue documentation
```

`.tools/`, `.nxf/`, and `.nf-test/` are local development state and must not be committed.

## Making changes

Keep patches focused. In particular:

- A behavior change should include or update tests that demonstrate the intended behavior.
- A bug fix should include a regression test when the failure can be reproduced reliably.
- Do not mix unrelated refactors into a behavior patch.
- Treat deletion as the risky operation: changes that weaken conservative retention semantics require explicit justification and coverage.
- Prefer supported Nextflow plugin APIs and extension points over coupling to internal implementation details.
- Avoid silent fallback behavior when the safe outcome can be made explicit.
- Update documentation when externally observable behavior, compatibility, or the development workflow changes.

## Updating the pinned toolchain

Toolchain upgrades are deliberate compatibility changes. When updating Java, Nextflow, or nf-test:

1. Change the pinned version and download artifact in `scripts/bootstrap-dev.sh` and, where applicable, `env.sh`.
2. Update the expected checksum for artifacts whose checksum is pinned in the repository.
3. Bootstrap from a clean `.tools/` directory.
4. Run the complete Gradle and nf-test checks.
5. Update compatibility or contributor documentation if the supported environment changes.

Do not replace pinned versions with `latest` URLs.

## Releasing

Public releases are tag-driven. The plugin version in `build.gradle` is paired with the corresponding `v`-prefixed Git tag, for example plugin `0.1.0` is released from tag `v0.1.0`.

The GitHub Actions workflow is the release gate:

1. Update `version` in `build.gradle` and add the release notes to `CHANGELOG.md`.
2. Run `./test.sh` locally.
3. Commit and push the release candidate, then wait for the `test` job to pass on that exact commit.
4. Confirm that the plugin claim is ready in the Nextflow Registry and that the repository Actions secret `NPR_API_KEY` is configured.
5. Tag the tested commit and push the tag, for example:

   ```bash
   git tag -a v0.1.0 -m "nf-gc 0.1.0"
   git push origin v0.1.0
   ```

A pushed `v*` tag runs the same test job again. The `release` job has `needs: test`, so it cannot publish unless that tag's exact commit passes the full regression suite. The release job also verifies that the tag exactly matches the plugin version before it:

- publishes the immutable plugin artifact to the Nextflow Registry using `NPR_API_KEY`;
- creates the matching GitHub Release from the corresponding `CHANGELOG.md` section.

Do not move or reuse a published version tag. After release, verify that a clean Nextflow environment can resolve the released plugin version from the Registry.

Registry credentials belong in GitHub Actions secrets or another external secret store. Never commit an API key or a repository-local `gradle.properties`.

## Before submitting a pull request

Verify at minimum:

- `./test.sh` passes.
- Behavior changes have appropriate test coverage.
- Relevant documentation is updated.
- No generated build output, tool downloads, Nextflow state, credentials, or private data are included.

For bugs and feature requests, see [issues.md](issues.md).
