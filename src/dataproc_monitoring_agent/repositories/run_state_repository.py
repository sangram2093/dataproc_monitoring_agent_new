"""BigQuery accessors for cached Spark run state records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

from google.api_core import exceptions
from google.cloud import bigquery

from ..config.settings import MonitoringConfig


@dataclass(slots=True)
class SparkRunState:
    """Domain object representing a single Spark application execution."""

    run_date: str | None
    application_start_time: str | None
    application_end_time: str | None
    status: str | None
    dataproc_jobid: str | None
    dataproc_cluster_uuid: str | None
    spark_taskid: str | None
    spark_jobid: str | None
    cluster_config_details: dict[str, Any]
    log_location: str | None
    application_id: str | None
    spark_event_metrics: dict[str, Any]

    @classmethod
    def from_row(cls, row: bigquery.table.Row) -> "SparkRunState":
        return cls(
            run_date=_to_iso(row.get("run_date")),
            application_start_time=_to_iso(row.get("application_start_time")),
            application_end_time=_to_iso(row.get("application_end_time")),
            status=row.get("status"),
            dataproc_jobid=row.get("dataproc_jobid"),
            dataproc_cluster_uuid=row.get("dataproc_cluster_uuid"),
            spark_taskid=row.get("spark_taskid"),
            spark_jobid=row.get("spark_jobid"),
            cluster_config_details=_coerce_json(row.get("cluster_config_details")),
            log_location=row.get("log_location"),
            application_id=row.get("application_id"),
            spark_event_metrics=_coerce_json(row.get("spark_event_metrics")),
        )

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "SparkRunState":
        return cls(
            run_date=payload.get("run_date"),
            application_start_time=payload.get("application_start_time"),
            application_end_time=payload.get("application_end_time"),
            status=payload.get("status"),
            dataproc_jobid=payload.get("dataproc_jobid"),
            dataproc_cluster_uuid=payload.get("dataproc_cluster_uuid"),
            spark_taskid=payload.get("spark_taskid"),
            spark_jobid=payload.get("spark_jobid"),
            cluster_config_details=_coerce_json(payload.get("cluster_config_details")),
            log_location=payload.get("log_location"),
            application_id=payload.get("application_id"),
            spark_event_metrics=_coerce_json(payload.get("spark_event_metrics")),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "run_date": self.run_date,
            "application_start_time": self.application_start_time,
            "application_end_time": self.application_end_time,
            "status": self.status,
            "dataproc_jobid": self.dataproc_jobid,
            "dataproc_cluster_uuid": self.dataproc_cluster_uuid,
            "spark_taskid": self.spark_taskid,
            "spark_jobid": self.spark_jobid,
            "cluster_config_details": self.cluster_config_details,
            "log_location": self.log_location,
            "application_id": self.application_id,
            "spark_event_metrics": self.spark_event_metrics,
        }

    @property
    def cluster_name(self) -> str:
        return (self.dataproc_jobid or "").strip()

    @property
    def primary_job_id(self) -> str:
        for candidate in (self.spark_jobid, self.application_id, self.spark_taskid):
            if candidate:
                return candidate
        return ""

    @property
    def duration_seconds(self) -> float | None:
        if not self.application_start_time or not self.application_end_time:
            return None
        try:
            start = datetime.fromisoformat(self.application_start_time)
            end = datetime.fromisoformat(self.application_end_time)
        except ValueError:
            return None
        if not start.tzinfo:
            start = start.replace(tzinfo=timezone.utc)
        if not end.tzinfo:
            end = end.replace(tzinfo=timezone.utc)
        return max((end - start).total_seconds(), 0.0)

    @property
    def app_metrics(self) -> dict[str, Any]:
        return _ensure_dict(self.spark_event_metrics.get("app"))

    @property
    def job_metrics(self) -> list[dict[str, Any]]:
        jobs = self.spark_event_metrics.get("jobs")
        if isinstance(jobs, list):
            return [
                dict(item) if isinstance(item, dict) else {
                    "value": item
                }
                for item in jobs
            ]
        return []

    @property
    def cost_summary(self) -> dict[str, Any]:
        app = self.app_metrics
        return {
            "app_vcore_seconds": app.get("app_vcore_seconds"),
            "app_memory_gb_seconds": app.get("app_memory_gb_seconds"),
            "executor_peak": app.get("executor_peak"),
        }


def fetch_run_state_records(
    config: MonitoringConfig,
    *,
    start_time: datetime,
    end_time: datetime,
) -> list[SparkRunState]:
    """Fetch Spark application run records within the supplied window."""

    client = bigquery.Client(project=config.project_id)

    query = f"""
        SELECT
          run_date,
          application_start_time,
          application_end_time,
          status,
          dataproc_jobid,
          dataproc_cluster_uuid,
          spark_taskid,
          spark_jobid,
          cluster_config_details,
          log_location,
          application_id,
          spark_event_metrics
        FROM `{config.fully_qualified_run_state_table}`
        WHERE (
            application_start_time BETWEEN @window_start AND @window_end
        )
        OR (
            application_start_time IS NULL
            AND run_date BETWEEN DATE(@window_start) AND DATE(@window_end)
        )
        ORDER BY application_start_time DESC NULLS LAST,
                 application_end_time DESC NULLS LAST
    """

    params = [
        bigquery.ScalarQueryParameter(
            "window_start",
            "TIMESTAMP",
            _to_query_timestamp(start_time),
        ),
        bigquery.ScalarQueryParameter(
            "window_end",
            "TIMESTAMP",
            _to_query_timestamp(end_time),
        ),
    ]

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    if config.bq_location:
        job_config.location = config.bq_location

    try:
        rows = client.query(query, job_config=job_config).result()
    except exceptions.GoogleAPICallError as exc:
        raise RuntimeError(
            "Failed to query Spark run state table: {table}: {exc}".format(
                table=config.fully_qualified_run_state_table,
                exc=exc,
            )
        ) from exc

    return [SparkRunState.from_row(row) for row in rows]


def _coerce_json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        return _ensure_dict(parsed)
    return _ensure_dict(value)


def _ensure_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    return {"value": value}


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if not value.tzinfo:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive
        return None


def _to_query_timestamp(moment: datetime) -> str:
    if not moment.tzinfo:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat()
