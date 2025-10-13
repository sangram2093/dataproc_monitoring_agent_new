"""Status report builder for the Dataproc monitoring agent."""

from __future__ import annotations

from collections import Counter
from typing import Iterable

from ..repositories.bigquery_repository import DataprocFact


def build_status_report(facts: Iterable[DataprocFact]) -> str:
    facts = list(facts)
    if not facts:
        return "No Dataproc activity detected within the configured window."

    status_counts = Counter(fact.job_state for fact in facts)
    anomalies = [
        fact for fact in facts if fact.anomaly_flags.get("has_issues")
    ]

    lines: list[str] = []
    lines.append("Dataproc monitoring summary")
    lines.append("==========================")
    lines.append(
        "Jobs ingested: {count} (states: {states})".format(
            count=len(facts),
            states=", ".join(f"{state}={count}" for state, count in status_counts.items()),
        )
    )

    if anomalies:
        severity_rank = {"critical": 0, "warning": 1, "info": 2, "default": 3}
        grouped: dict[str, dict[str, object]] = {}
        for fact in anomalies:
            findings = fact.anomaly_flags.get("findings", [])
            if not findings:
                continue
            job_family = fact.anomaly_flags.get("job_family") or fact.job_id
            for finding in findings:
                severity = finding.get("severity", "default")
                rank = severity_rank.get(severity, severity_rank["default"])
                current = grouped.get(job_family)
                if current is None or rank < current["rank"]:
                    grouped[job_family] = {
                        "rank": rank,
                        "severity": severity,
                        "finding": finding,
                        "fact": fact,
                    }

        lines.append("")
        lines.append(f"⚠️  {len(grouped)} regression(s) detected across logical jobs:")
        for job_family, payload in sorted(grouped.items(), key=lambda item: item[1]["rank"]):
            fact = payload["fact"]
            finding = payload["finding"]
            run_identifier = fact.anomaly_flags.get("run_identifier") or fact.job_id
            baseline = fact.anomaly_flags.get("baseline_reference") or {}
            baseline_snippet = ""
            if baseline.get("p50_duration") and baseline.get("run_count"):
                baseline_snippet = (
                    " (baseline median {median:.1f}s over {count} runs)"
                ).format(
                    median=baseline["p50_duration"],
                    count=baseline["run_count"],
                )
            lines.append(
                f"- {job_family} (latest run {run_identifier}) on cluster {fact.cluster_name}: {finding['message']}{baseline_snippet}"
            )
    else:
        lines.append("")
        lines.append("No runtime regressions detected against current baselines.")

    if anomalies:
        lines.append("")
        lines.append("Suggested actions:")
        raw_actions: list[tuple[str, str]] = []
        for job_family, payload in sorted(grouped.items(), key=lambda item: item[1]["rank"]):
            fact = payload["fact"]
            finding = payload["finding"]
            action_text = finding.get("action")
            if not action_text and fact.anomaly_flags.get("recommendations"):
                action_text = fact.anomaly_flags["recommendations"][0]
            if action_text:
                raw_actions.append((job_family, action_text))
        for fact in anomalies:
            family = fact.anomaly_flags.get("job_family") or fact.job_id
            for recommendation in fact.anomaly_flags.get("recommendations", []):
                raw_actions.append((family, recommendation))

        if raw_actions:
            seen: set[str] = set()
            for family, action in raw_actions:
                key = f"{family}::{action}"
                if key in seen:
                    continue
                seen.add(key)
                lines.append(f"- {family}: {action}")
        else:
            lines.append("- Monitor upcoming runs; no actionable regressions flagged.")

    lines.append("")
    lines.append("Recent job highlights:")
    for fact in facts[:5]:
        duration = "n/a"
        if fact.duration_seconds:
            duration = f"{fact.duration_seconds:.1f}s"
        run_identifier = fact.anomaly_flags.get("run_identifier") or fact.job_id
        lines.append(
            f"- {run_identifier} ({fact.job_type}) state={fact.job_state} duration={duration}"
        )
        cost_summary = fact.anomaly_flags.get("cost_summary", {})
        vcores = cost_summary.get("app_vcore_seconds")
        memory = cost_summary.get("app_memory_gb_seconds")
        if any(value is not None for value in (vcores, memory)):
            vcores_str = f"{float(vcores):.1f}" if isinstance(vcores, (int, float)) else vcores
            memory_str = f"{float(memory):.1f}" if isinstance(memory, (int, float)) else memory
            lines.append(
                f"  resource usage: vcore_seconds={vcores_str} memory_gb_seconds={memory_str}"
            )

    return "\n".join(lines)
