# Dataproc Monitoring Agent (Google ADK)

This project packages a Dataproc-focused agent built with the Google Agent Development Kit (ADK). The agent ingests recent telemetry from Dataproc, computes baselines in BigQuery, flags regressions, and returns an operator-ready status report. The structure is ready to slot into a broader orchestrator + specialist framework by adding sibling agents for other Google Cloud services.

## Features

- **Signal collection** – queries the Composer-populated `cag_run_state` BigQuery table to retrieve Spark application telemetry (cluster configs, driver log locations, event metrics).
- **Performance memory** – writes daily fact rows to BigQuery with runtime metrics, cost indicators (vcore/memory seconds), and anomaly flags.
- **Baseline analysis** – loads trailing P50/P95 duration and cost benchmarks, then flags runtime, compute-cost, and task-skew regressions per logical Spark job family.
- **Right-sizing insights** – reviews executor usage versus provisioned workers to call out idle clusters or capacity limits.
- **Actionable findings** – each detected regression includes baseline deltas, the impacted stage/job context, and concrete follow-up recommendations surfaced in reports and anomaly flags.
- **Agentic workflow** – orchestrator agent delegates to specialist sub-agents (collector → memory builder → reporter) using ADK tooling.

## Repository layout

```
src/
  dataproc_monitoring_agent/
    analytics/          # Baseline + anomaly logic
    agents/             # ADK agent definitions
    config/             # Environment-driven configuration helpers
    reporting/          # Textual status report builder
    repositories/       # BigQuery persistence layer
    services/           # GCP API clients (Dataproc, Monitoring, Logging, Storage)
    tools/              # Tool functions exposed to ADK
    runner.py           # CLI + Runner integration
    __main__.py         # Enables `python -m dataproc_monitoring_agent`
```

## Prerequisites

- Python 3.10+
- [Google Cloud SDK](https://cloud.google.com/sdk) configured with access to BigQuery (read from the run state table, write to the performance table).
- Application Default Credentials (`gcloud auth application-default login`) or `GOOGLE_APPLICATION_CREDENTIALS` pointing to a service account key with the necessary roles.
- BigQuery dataset write access for the configured table.
- Composer (or equivalent) workflow that populates the `cag_run_state` table with Spark application metrics before the agent runs.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

The agent reads configuration from environment variables. The required minimum is the project and region.

| Variable | Purpose |
| --- | --- |
| `DATAPROC_PROJECT_ID` | Target Google Cloud project. |
| `DATAPROC_REGION` | Primary Dataproc region (e.g. `us-central1`). |
| `DATAPROC_LOOKBACK_HOURS` | Lookback window for ingestion (default `24`). |
| `DATAPROC_BASELINE_DAYS` | Trailing window for baseline stats (default `7`). |
| `DATAPROC_BQ_DATASET` / `DATAPROC_BQ_TABLE` | Location of the BigQuery performance table. |
| `DATAPROC_RUN_STATE_DATASET` / `DATAPROC_RUN_STATE_TABLE` | Dataset/table containing the cached Spark run state (default dataset falls back to `DATAPROC_BQ_DATASET`, table defaults to `cag_run_state`). |
| `DATAPROC_BQ_LOCATION` | Optional BigQuery dataset location. |
| `DATAPROC_DRY_RUN` | Set to `true` to skip BigQuery writes while developing. |
| `DATAPROC_AGENT_MODEL` | Optional override for the Gemini model used by the agents (default `models/gemini-1.5-pro`). |

## Usage

Run a monitoring cycle and print the generated report:

```bash
python -m dataproc_monitoring_agent \
  --prompt "Run the Dataproc monitoring playbook and summarise key regressions."
```

The orchestrator agent will execute these stages:

1. `dataproc_collector` → `ingest_dataproc_signals`
2. `performance_memory_builder` → `build_performance_memory`
3. `dataproc_reporter` → `generate_dataproc_report`

### Programmatic invocation

```python
from dataproc_monitoring_agent import run_once

report = run_once()
print(report)
```

### BigQuery schema

`repositories/bigquery_repository.DataprocFact` documents the persisted schema.

**BigQuery setup (manual)**

The agent only verifies the dataset/table and will not create them automatically. Before running the pipeline:

1. Create the dataset `DATAPROC_BQ_DATASET` in project `DATAPROC_PROJECT_ID` (example: `bq --project_id=$Env:DATAPROC_PROJECT_ID mk --location=$Env:DATAPROC_BQ_LOCATION $Env:DATAPROC_BQ_DATASET`).
2. Create the table `DATAPROC_BQ_DATASET.DATAPROC_BQ_TABLE` using the schema documented in `repositories/bigquery_repository.DataprocFact` (for example: `bq mk --table ${Env:DATAPROC_PROJECT_ID}:$Env:DATAPROC_BQ_DATASET.$Env:DATAPROC_BQ_TABLE schemas/dataproc_fact.json`).
3. Grant the runtime service account at least `bigquery.tables.get`, `bigquery.tables.list`, and `bigquery.dataEditor` on the table.

If the dataset/table are missing or inaccessible the agent raises a readable error and no creation attempt is made.

## Testing

A lightweight smoke test can verify configuration loading without touching Google Cloud:

```bash
pytest
```

*(Add credentials-mocking tests as you expand the pipeline.)*

## Extending the framework

- Add specialist agents for Cloud Composer, BigQuery, Dataplex, etc., then attach them as additional `sub_agents` to the orchestrator.
- Introduce guard-rail actions (e.g., autoscaling) behind dedicated tools that require human approval before execution.
- Swap `InMemorySessionService` / `InMemoryArtifactService` with persistent implementations when hosting the agent long-running.

## Notes

- The agent trusts the `cag_run_state` dataset as ground truth; ensure your Composer pipeline writes the row before Dataproc clusters are torn down.
- Cost fields (vcore seconds, memory seconds) flow straight from `spark_event_metrics`; adjust your upstream JSON schema if you need additional signals.
- Legacy GCP service clients remain in `src/dataproc_monitoring_agent/services/` for backward compatibility but are no longer invoked by the default pipeline.
- Logical Spark job families are inferred by trimming the run-specific suffix from `spark_jobid`/`spark_taskid`, so baseline comparisons span multiple executions of the same job definition.
