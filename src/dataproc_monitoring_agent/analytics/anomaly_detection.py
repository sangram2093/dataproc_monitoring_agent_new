"""Simple heuristic-based anomaly detectors for Dataproc runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..analytics.performance_memory import BaselineStats
from ..repositories.bigquery_repository import DataprocFact


@dataclass(slots=True)
class Anomaly:
    kind: str
    severity: str
    message: str
    evidence: dict


def detect_job_runtime_anomaly(
    fact: DataprocFact,
    baseline: BaselineStats | None,
) -> Anomaly | None:
    """Spot jobs exceeding their historical runtime baselines."""

    if not fact.duration_seconds or fact.duration_seconds <= 0:
        return None
    if not baseline or not baseline.p50_duration:
        return None

    ratio = fact.duration_seconds / baseline.p50_duration
    if ratio < 1.5:
        return None

    severity = "warning"
    if ratio >= 2.0:
        severity = "critical"

    return Anomaly(
        kind="job_runtime_regression",
        severity=severity,
        message=(
            f"Job {fact.job_id} runtime {fact.duration_seconds:.1f}s is "
            f"{ratio:.1f}x slower than median ({baseline.p50_duration:.1f}s)."
        ),
        evidence={
            "job_id": fact.job_id,
            "duration_seconds": fact.duration_seconds,
            "baseline_p50_seconds": baseline.p50_duration,
            "baseline_p95_seconds": baseline.p95_duration,
            "baseline_run_count": baseline.run_count,
        },
    )


def detect_task_straggler(
    fact: DataprocFact,
    spark_metrics: dict[str, Any] | None,
) -> Anomaly | None:
    """Highlight Spark jobs with significant task skew."""

    if not spark_metrics:
        return None

    jobs = spark_metrics.get("jobs")
    if not isinstance(jobs, list):
        return None

    candidate: dict[str, Any] | None = None
    ratio_value: float | None = None

    for job in jobs:
        if not isinstance(job, dict):
            continue
        ratio_raw = job.get("max_over_median_ratio")
        try:
            ratio = float(ratio_raw)
        except (TypeError, ValueError):
            continue
        if ratio <= 3.0:
            continue
        if not candidate or ratio > (ratio_value or 0.0):
            candidate = job
            ratio_value = ratio

    if not candidate or ratio_value is None:
        return None

    severity = "warning"
    if ratio_value >= 5.0:
        severity = "critical"

    job_identifier = candidate.get("job_id") or fact.job_id

    return Anomaly(
        kind="task_straggler_detected",
        severity=severity,
        message=(
            f"Spark job {job_identifier} shows task skew (max/median ratio {ratio_value:.2f})."
        ),
        evidence={
            "job_id": job_identifier,
            "ratio": ratio_value,
            "num_tasks": candidate.get("num_tasks"),
            "p95_task_duration_ms": candidate.get("p95_task_duration_ms"),
            "max_task_duration_ms": candidate.get("max_task_duration_ms"),
        },
    )


def synthesize_anomaly_flags(
    fact: DataprocFact,
    *,
    baseline: BaselineStats | None,
    spark_metrics: dict[str, Any] | None = None,
    cost_summary: dict[str, Any] | None = None,
) -> dict:
    """Aggregate anomalies into a JSON-ready representation."""

    findings: list[Anomaly] = []

    runtime_anomaly = detect_job_runtime_anomaly(fact, baseline)
    if runtime_anomaly:
        findings.append(runtime_anomaly)

    straggler = detect_task_straggler(fact, spark_metrics)
    if straggler:
        findings.append(straggler)

    cost_payload = {
        key: value
        for key, value in (cost_summary or {}).items()
        if value is not None
    }

    return {
        "findings": [
            {
                "kind": finding.kind,
                "severity": finding.severity,
                "message": finding.message,
                "evidence": finding.evidence,
            }
            for finding in findings
        ],
        "cost_summary": cost_payload,
        "has_issues": any(finding.severity in {"warning", "critical"} for finding in findings),
    }
