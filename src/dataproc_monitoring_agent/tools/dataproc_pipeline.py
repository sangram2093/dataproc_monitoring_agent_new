"""Tool functions orchestrating the Dataproc monitoring pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from google.adk.tools.tool_context import ToolContext

from ..analytics.anomaly_detection import synthesize_anomaly_flags
from ..analytics.performance_memory import load_baselines
from ..config.settings import MonitoringConfig, load_config
from ..reporting.report_builder import build_status_report
from ..repositories.bigquery_repository import (
    DataprocFact,
    ensure_performance_table,
    insert_daily_facts,
    utc_now,
)
from ..repositories.run_state_repository import (
    SparkRunState,
    fetch_run_state_records,
)


def ingest_dataproc_signals(
    *,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
    lookback_hours: Optional[int] = None,
    tool_context: Optional[ToolContext] = None,
) -> dict[str, Any]:
    """Collect Spark run state snapshots sourced from BigQuery."""

    config = _resolve_config(
        project_id=project_id,
        region=region,
        lookback_hours=lookback_hours,
    )
    end_time = utc_now()
    start_time = end_time - config.lookback

    run_states = fetch_run_state_records(
        config,
        start_time=start_time,
        end_time=end_time,
    )

    ingestion_payload = {
        "window": {
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
        },
        "runs": [run.to_payload() for run in run_states],
    }

    if tool_context is not None:
        tool_context.state["dataproc_ingestion"] = ingestion_payload

    distinct_clusters = {
        run.cluster_name
        for run in run_states
        if run.cluster_name
    }

    return {
        "window": ingestion_payload["window"],
        "run_count": len(run_states),
        "distinct_clusters": len(distinct_clusters),
    }


def build_performance_memory(
    *,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
    tool_context: Optional[ToolContext] = None,
) -> dict[str, Any]:
    """Persist Spark job observations into BigQuery with anomaly flags."""

    config = _resolve_config(project_id=project_id, region=region)
    ingestion_payload = None
    if tool_context is not None:
        ingestion_payload = tool_context.state.get("dataproc_ingestion")
    if not ingestion_payload:
        message = (
            "No run state payload is cached yet. Run ingest_dataproc_signals before "
            "building performance memory."
        )
        return {
            "persisted_rows": 0,
            "dry_run": config.dry_run,
            "has_anomalies": False,
            "message": message,
        }

    run_payloads = ingestion_payload.get("runs", [])
    if not run_payloads:
        message = (
            "No Spark run state records were ingested for the requested window. "
            "Confirm the cag_run_state table is populated before persisting performance memory."
        )
        return {
            "persisted_rows": 0,
            "dry_run": config.dry_run,
            "has_anomalies": False,
            "message": message,
        }

    run_states = [SparkRunState.from_payload(payload) for payload in run_payloads]

    ensure_performance_table(config)

    now = utc_now()
    baselines = load_baselines(
        config,
        as_of=now,
        trailing_window=config.baseline_window,
    )

    facts: list[DataprocFact] = []
    has_anomaly = False
    for run_state in run_states:
        fact, fact_has_issue = _build_fact(
            config=config,
            as_of=now,
            run_state=run_state,
            baselines=baselines,
        )
        if fact_has_issue:
            has_anomaly = True
        facts.append(fact)

    insert_daily_facts(config, records=facts)

    serialized = [fact.to_json() for fact in facts]
    if tool_context is not None:
        tool_context.state["dataproc_facts"] = serialized

    return {
        "persisted_rows": len(serialized),
        "dry_run": config.dry_run,
        "has_anomalies": has_anomaly,
    }


def generate_dataproc_report(
    *,
    tool_context: Optional[ToolContext] = None,
) -> dict[str, Any]:
    """Return a human readable Dataproc status report."""

    facts_payload = None
    if tool_context is not None:
        facts_payload = tool_context.state.get("dataproc_facts")

    if not facts_payload:
        report = (
            "No Dataproc facts are cached yet. Run build_performance_memory before requesting a report."
        )
        if tool_context is not None:
            tool_context.state["dataproc_report"] = report
        return {"report": report}

    report = build_status_report(
        [
            DataprocFact(**fact)
            for fact in facts_payload
        ]
    )

    if tool_context is not None:
        tool_context.state["dataproc_report"] = report

    return {"report": report}


def _resolve_config(**overrides: Any) -> MonitoringConfig:
    usable_overrides = {
        key: value
        for key, value in overrides.items()
        if value is not None
    }
    if usable_overrides:
        return load_config(usable_overrides)
    return load_config()


def _build_fact(
    *,
    config: MonitoringConfig,
    as_of: datetime,
    run_state: SparkRunState,
    baselines: Dict[str, Any],
) -> Tuple[DataprocFact, bool]:
    job_id = run_state.primary_job_id
    duration_seconds = run_state.duration_seconds

    fact = DataprocFact(
        ingest_date=as_of.date().isoformat(),
        ingest_timestamp=as_of.isoformat(),
        project_id=config.project_id,
        region=config.region,
        cluster_name=run_state.cluster_name or "unknown",
        job_id=job_id,
        job_type="SPARK",
        job_state=run_state.status or "UNKNOWN",
        job_start_time=run_state.application_start_time,
        job_end_time=run_state.application_end_time,
        duration_seconds=duration_seconds,
        yarn_application_ids=[run_state.application_id]
        if run_state.application_id
        else [],
        cluster_metrics={
            "cluster_config_details": run_state.cluster_config_details,
            "dataproc_cluster_uuid": run_state.dataproc_cluster_uuid,
        },
        job_metrics=run_state.spark_event_metrics,
        driver_log_excerpt=run_state.log_location,
        yarn_log_excerpt=None,
        spark_event_snippet=_format_spark_event_snippet(
            run_state.spark_event_metrics
        ),
        anomaly_flags={},
    )

    baseline = baselines.get(job_id)
    anomaly_payload = synthesize_anomaly_flags(
        fact,
        baseline=baseline,
        spark_metrics=run_state.spark_event_metrics,
        cost_summary=run_state.cost_summary,
    )
    has_issues = bool(anomaly_payload.get("has_issues")) if anomaly_payload else False
    fact.anomaly_flags = anomaly_payload or {}
    return fact, has_issues


def _format_spark_event_snippet(metrics: dict[str, Any]) -> str | None:
    if not metrics:
        return None

    app = metrics.get("app") if isinstance(metrics.get("app"), dict) else None
    jobs = metrics.get("jobs") if isinstance(metrics.get("jobs"), list) else []

    lines: list[str] = []
    if app:
        app_id = app.get("app_id") or app.get("application_id")
        app_name = app.get("app_name") or app.get("name")
        duration_ms = app.get("app_duration_ms") or app.get("duration_ms")
        executor_peak = app.get("executor_peak")
        vcore_seconds = app.get("app_vcore_seconds")
        memory_seconds = app.get("app_memory_gb_seconds")
        lines.append(
            "Application {app_name} ({app_id}) duration={duration_ms}ms executors={executor_peak}".format(
                app_name=app_name or "unknown",
                app_id=app_id or "n/a",
                duration_ms=duration_ms if duration_ms is not None else "n/a",
                executor_peak=executor_peak if executor_peak is not None else "n/a",
            )
        )
        if vcore_seconds is not None or memory_seconds is not None:
            lines.append(
                "  Resource usage: vcore_seconds={vcores} memory_gb_seconds={memory}".format(
                    vcores=_format_metric_value(vcore_seconds),
                    memory=_format_metric_value(memory_seconds),
                )
            )

    if jobs:
        first_job = next(
            (job for job in jobs if isinstance(job, dict)),
            None,
        )
        if first_job:
            lines.append(
                "  Job {job_id} tasks={tasks} duration={duration}ms p95={p95}ms".format(
                    job_id=first_job.get("job_id", "n/a"),
                    tasks=first_job.get("num_tasks", "n/a"),
                    duration=first_job.get("job_duration_ms", "n/a"),
                    p95=first_job.get("p95_task_duration_ms", "n/a"),
                )
            )
            ratio = first_job.get("max_over_median_ratio")
            if ratio is not None:
                lines.append(
                    "    Straggler ratio: {ratio}".format(
                        ratio=_format_metric_value(ratio),
                    )
                )

    if not lines:
        return None

    combined = "\n".join(lines)
    return combined[:6000]


def _format_metric_value(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return f"{value:.2f}"
    return value
