#!/usr/bin/env python3
# Copyright 2026 Mert Akin
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import csv
import os
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

EXPECTED_TASKS = 234
EXPECTED_DELETIONS = 171


@dataclass(frozen=True)
class Deletion:
    line: int
    process: str
    path: Path

    @property
    def identity(self) -> tuple[str, str]:
        # Work hashes differ between independent runs. Process + basename is the
        # stable third-party identity here; Counter preserves repeated outputs
        # such as per-task versions.yml files.
        return self.process, self.path.name


@dataclass
class RunEvidence:
    name: str
    mode: str
    deletions: list[Deletion]
    early_deletions: list[Deletion]
    trace_tasks: int

    @property
    def deletion_multiset(self) -> Counter[tuple[str, str]]:
        return Counter(item.identity for item in self.deletions)


def fail(message: str) -> None:
    raise SystemExit(f"nf-core/rnaseq acceptance failed: {message}")


def parse_events(case_dir: Path, expected_mode: str) -> RunEvidence:
    events_path = case_dir / "nf-gc-events.tsv"
    if not events_path.is_file():
        fail(f"{case_dir.name}: missing {events_path.name}")

    mode: str | None = None
    deletions: list[Deletion] = []
    task_complete_lines: list[int] = []
    flow_complete_lines: list[int] = []
    delete_failures: list[str] = []

    with events_path.open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            fields = raw.rstrip("\n").split("\t")
            event = fields[0] if fields else ""
            if event == "GC_MODE" and len(fields) >= 2:
                mode = fields[1]
            elif event == "TASK_COMPLETE":
                task_complete_lines.append(line_number)
            elif event == "ARTIFACT_DELETED" and len(fields) >= 3:
                deletions.append(
                    Deletion(
                        line=line_number,
                        process=fields[1],
                        path=Path(fields[2]),
                    )
                )
            elif event == "ARTIFACT_DELETE_FAILED":
                delete_failures.append(raw.rstrip("\n"))
            elif event == "FLOW_COMPLETE":
                flow_complete_lines.append(line_number)

    if mode != expected_mode:
        fail(f"{case_dir.name}: expected gc mode {expected_mode!r}, observed {mode!r}")
    if len(flow_complete_lines) != 1:
        fail(f"{case_dir.name}: expected one FLOW_COMPLETE, observed {len(flow_complete_lines)}")
    if not task_complete_lines:
        fail(f"{case_dir.name}: no TASK_COMPLETE events")
    if delete_failures:
        fail(
            f"{case_dir.name}: observed {len(delete_failures)} ARTIFACT_DELETE_FAILED events; "
            f"first={delete_failures[0]!r}"
        )

    last_task_complete = max(task_complete_lines)
    early = [item for item in deletions if item.line < last_task_complete]

    trace_path = case_dir / "trace.tsv"
    with trace_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    if len(rows) != EXPECTED_TASKS:
        fail(f"{case_dir.name}: expected {EXPECTED_TASKS} tasks, observed {len(rows)}")
    bad = [row for row in rows if row.get("status") != "COMPLETED" or row.get("exit") != "0"]
    if bad:
        fail(f"{case_dir.name}: {len(bad)} trace tasks were not COMPLETED with exit 0")

    if len(deletions) != EXPECTED_DELETIONS:
        fail(
            f"{case_dir.name}: expected {EXPECTED_DELETIONS} reclaimed artifacts, "
            f"observed {len(deletions)}"
        )

    results_dir = case_dir / "results"
    broken_symlinks: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(results_dir, followlinks=False):
        for name in [*dirnames, *filenames]:
            path = Path(dirpath) / name
            if path.is_symlink() and not path.exists():
                broken_symlinks.append(path)
    if broken_symlinks:
        relative = broken_symlinks[0].relative_to(case_dir)
        fail(
            f"{case_dir.name}: observed {len(broken_symlinks)} broken result symlinks; "
            f"first={relative}"
        )

    multiqc = results_dir / "multiqc" / "star_salmon" / "multiqc_report.html"
    if not multiqc.is_file() or multiqc.stat().st_size == 0:
        fail(f"{case_dir.name}: missing/non-empty MultiQC report")

    final_bams = list((case_dir / "results" / "star_salmon").glob("*.markdup.sorted.bam"))
    if len(final_bams) != 5 or any(path.stat().st_size == 0 for path in final_bams):
        fail(f"{case_dir.name}: expected five non-empty published markdup BAMs")

    return RunEvidence(
        name=case_dir.name,
        mode=mode,
        deletions=deletions,
        early_deletions=early,
        trace_tasks=len(rows),
    )


def require_same_reclaim_set(reference: RunEvidence, other: RunEvidence) -> None:
    if reference.deletion_multiset == other.deletion_multiset:
        return

    missing = reference.deletion_multiset - other.deletion_multiset
    extra = other.deletion_multiset - reference.deletion_multiset
    fail(
        f"eventual reclaim-set mismatch: {reference.name} vs {other.name}; "
        f"missing={sum(missing.values())}, extra={sum(extra.values())}"
    )


def require_critical_bams_early(run: RunEvidence) -> None:
    last_task_early = Counter(item.identity for item in run.early_deletions)

    def count(process_suffix: str, filename_suffix: str) -> int:
        return sum(
            amount
            for (process, basename), amount in last_task_early.items()
            if process.endswith(process_suffix) and basename.endswith(filename_suffix)
        )

    checks = [
        (":ALIGN_STAR:STAR_ALIGN", ".Aligned.out.bam", 5, "STAR genomic BAM"),
        (":ALIGN_STAR:STAR_ALIGN", ".Aligned.toTranscriptome.out.bam", 5, "STAR transcriptome BAM"),
        (
            ":ALIGN_STAR:BAM_SORT_STATS_SAMTOOLS:SAMTOOLS_SORT",
            ".sorted.bam",
            5,
            "SAMtools sorted intermediate BAM",
        ),
    ]

    for process_suffix, filename_suffix, expected, label in checks:
        observed = count(process_suffix, filename_suffix)
        if observed != expected:
            fail(
                f"{run.name}: expected {expected} {label} deletions before the final task completed, "
                f"observed {observed}"
            )


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: verify-rnaseq-acceptance.py <acceptance-root>")

    root = Path(sys.argv[1]).resolve()
    process_link = parse_events(root / "process-link", "process")
    artifact_link = parse_events(root / "artifact-link", "artifact")
    artifact_copy = parse_events(root / "artifact-copy", "artifact")

    # All policies must converge on the same eventual reclaim set for the exact,
    # pinned third-party workload.
    require_same_reclaim_set(process_link, artifact_link)
    require_same_reclaim_set(artifact_link, artifact_copy)

    # Process mode is still Path-specific; it must reclaim at least some artifacts
    # before the final task instead of degenerating into flow-end-only cleanup.
    if not process_link.early_deletions:
        fail("process+link performed no reclamation before the final task completed")

    # Artifact mode exists to reclaim concrete artifacts earlier when the runtime
    # supplies exact evidence. The hard-link deployment is the production-relevant
    # path and must retain a real timing advantage over process mode.
    if len(artifact_link.early_deletions) <= len(process_link.early_deletions):
        fail(
            "artifact+link did not reclaim more artifacts before the final task "
            f"than process+link ({len(artifact_link.early_deletions)} <= "
            f"{len(process_link.early_deletions)})"
        )

    require_critical_bams_early(artifact_link)

    print("nf-core/rnaseq acceptance PASS")
    print(f"  process+link : {len(process_link.deletions)} total, {len(process_link.early_deletions)} mid-run")
    print(f"  artifact+link: {len(artifact_link.deletions)} total, {len(artifact_link.early_deletions)} mid-run")
    print(f"  artifact+copy: {len(artifact_copy.deletions)} total, {len(artifact_copy.early_deletions)} mid-run")
    print("  eventual reclaim sets: identical")
    print("  artifact+link critical STAR/SAMtools BAMs: reclaimed mid-run")


if __name__ == "__main__":
    main()
