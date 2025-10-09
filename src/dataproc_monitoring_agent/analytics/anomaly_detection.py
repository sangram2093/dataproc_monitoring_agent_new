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


def detect_cost_regression(
    fact: DataprocFact,
    baseline: BaselineStats | None,
    cost_summary: dict[str, Any] | None,
) -> Anomaly | None:
    """Detect rising compute cost versus historical norms."""

    if not baseline or not cost_summary:
        return None

    current_vcores = _safe_float(cost_summary.get("app_vcore_seconds"))
    current_memory = _safe_float(cost_summary.get("app_memory_gb_seconds"))

    findings: list[tuple[str, float, float]] = []

    if current_vcores is not None and baseline.avg_app_vcore_seconds:
        findings.append(("vcore_seconds", current_vcores, baseline.avg_app_vcore_seconds))

    if current_memory is not None and baseline.avg_app_memory_gb_seconds:
        findings.append(("memory_gb_seconds", current_memory, baseline.avg_app_memory_gb_seconds))

    dominant: tuple[str, float, float] | None = None
    ratio_dominant: float = 0.0

    for metric_name, current_value, baseline_value in findings:
        if not baseline_value or baseline_value <= 0:
            continue
        ratio = current_value / baseline_value
        if ratio <= 1.3:  # Allow modest variance
            continue
        if ratio > ratio_dominant:
            ratio_dominant = ratio
            dominant = (metric_name, current_value, baseline_value)

    if not dominant:
        return None

    metric_name, current_value, baseline_value = dominant
    severity = "warning" if ratio_dominant < 1.75 else "critical"

    return Anomaly(
        kind="cost_regression", 
        severity=severity,
        message=(
            f"{metric_name.replace('_', ' ')} increased to {current_value:.1f} (was {baseline_value:.1f} on average)."
        ),
        evidence={
            "metric": metric_name,
            "current": current_value,
            "baseline_average": baseline_value,
            "ratio": ratio_dominant,
        },
    )


def detect_cluster_rightsizing(
    fact: DataprocFact,
    cluster_profile: dict[str, Any] | None,
    cost_summary: dict[str, Any] | None,
) -> Anomaly | None:
    """Suggest cluster tuning based on executor usage versus provisioned workers."""

    if not cluster_profile or not cost_summary:
        return None

    total_workers = cluster_profile.get("total_workers")
    if not total_workers or total_workers <= 0:
        return None

    executor_peak = _safe_float(cost_summary.get("executor_peak"))
    if executor_peak is None:
        return None

    utilization = executor_peak / total_workers

    evidence = {
        "executor_peak": executor_peak,
        "total_workers": total_workers,
        "autoscaling_enabled": bool(cluster_profile.get("autoscaling_enabled")),
    }

    if utilization < 0.45:
        severity = "info"
        message = (
            f"Cluster {fact.cluster_name} used only {utilization:.0%} of provisioned workers (peak executors {executor_peak:.1f} vs {total_workers})."
        )
        return Anomaly(
            kind="cluster_underutilized",
            severity=severity,
            message=message,
            evidence=evidence,
        )

    if utilization >= 0.95:
        severity = "warning"
        message = (
            f"Cluster {fact.cluster_name} saturated provisioned workers (peak executors {executor_peak:.1f} vs {total_workers})."
        )
        return Anomaly(
            kind="cluster_capacity_near_limit",
            severity=severity,
            message=message,
            evidence=evidence,
        )

    return None


def synthesize_anomaly_flags(
    fact: DataprocFact,
    *,
    baseline: BaselineStats | None,
    spark_metrics: dict[str, Any] | None = None,
    cost_summary: dict[str, Any] | None = None,
    cluster_profile: dict[str, Any] | None = None,
    job_family: str | None = None,
    run_identifier: str | None = None,
) -> dict:
    """Aggregate anomalies into a JSON-ready representation."""

    findings: list[Anomaly] = []

    runtime_anomaly = detect_job_runtime_anomaly(fact, baseline)
    if runtime_anomaly:
        findings.append(runtime_anomaly)

    straggler = detect_task_straggler(fact, spark_metrics)
    if straggler:
        findings.append(straggler)

    cost_regression = detect_cost_regression(fact, baseline, cost_summary)
    if cost_regression:
        findings.append(cost_regression)

    cluster_hint = detect_cluster_rightsizing(fact, cluster_profile, cost_summary)
    if cluster_hint:
        findings.append(cluster_hint)

    cost_payload = {
        key: value
        for key, value in (cost_summary or {}).items()
        if value is not None
    }

    baseline_snapshot = {}
    if baseline:
        baseline_snapshot = {
            "p50_duration": baseline.p50_duration,
            "p95_duration": baseline.p95_duration,
            "avg_duration": baseline.avg_duration,
            "avg_app_vcore_seconds": baseline.avg_app_vcore_seconds,
            "avg_app_memory_gb_seconds": baseline.avg_app_memory_gb_seconds,
            "avg_max_over_median_ratio": baseline.avg_max_over_median_ratio,
            "p95_task_duration_ms": baseline.p95_task_duration_ms,
            "run_count": baseline.run_count,
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
        "baseline_reference": baseline_snapshot,
        "job_family": job_family,
        "run_identifier": run_identifier,
        "has_issues": any(finding.severity in {"warning", "critical"} for finding in findings),
    }


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
