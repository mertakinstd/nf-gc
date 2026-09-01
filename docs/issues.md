# Reporting issues

Before opening a new issue, search existing issues for the same failure or request. A small, reproducible report is substantially easier to diagnose than a full production workflow dump.

## Bug reports

Include the information needed to reproduce the problem:

- nf-gc version or commit
- Nextflow version
- Java version
- operating system and architecture
- exact command used
- minimal relevant workflow and configuration
- expected behavior
- actual behavior
- concise reproduction steps
- relevant log excerpt or error message

When reporting filesystem or garbage-collection behavior, also describe the artifact lifecycle:

- Which files existed before nf-gc acted?
- Which files were reclaimed?
- Which files were expected to survive?
- Were the affected files published outputs, cached artifacts, `storeDir` artifacts, terminal outputs, workflow outputs, or another category?
- Was workflow resume attempted after reclamation? If so, what happened?

Prefer a minimal synthetic reproducer when possible. Do not upload credentials, access tokens, private keys, confidential paths, sensitive datasets, or unredacted logs containing them.

## Feature requests

Describe the problem before proposing the implementation. Include:

- the current limitation
- the desired behavior
- a concrete use case
- why the existing conservative behavior is insufficient
- any compatibility or safety constraints that matter

Requests that alter retention or deletion semantics should make the intended safety boundary explicit.

## Logs and large artifacts

Provide only the section of a log needed to understand the issue. If a large workflow is required to reproduce the problem, reduce it to the smallest workflow that still exhibits the behavior before attaching or linking data.
