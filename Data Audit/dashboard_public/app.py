#!/usr/bin/env python3
"""Public LOB data-capture dashboard.

This dashboard intentionally reads only lightweight audit artifacts:
manifests, quality reports, and gap-patch reports. It does not expose raw LOB
JSONL files, logs, SSH details, or internal process output.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


ROOT = Path("/opt/lob_system")
MANIFEST_DIR = ROOT / "manifests"
QUALITY_DIR = ROOT / "reports" / "quality_beta"

SOURCE_ORDER = ["binance", "binance_backup", "coinbase", "coinbase_backup"]
STATUS_ORDER = {
    "OK": 0,
    "PATCH_OK": 0,
    "SEGMENTED": 1,
    "PATCH_PARTIAL": 1,
    "CAPTURE_GAP_UNRESOLVED": 2,
    "PATCH_FAILED_OR_UNRESOLVED": 2,
    "DIAGNOSTIC_ONLY": 2,
    "REJECT_CORRUPT_JSON": 3,
    "REJECT_NO_SNAPSHOT": 3,
    "NO_UPDATES": 3,
}


st.set_page_config(
    page_title="LOB Capture Monitor",
    page_icon="LOB",
    layout="wide",
)


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return {"_load_error": "json_decode_error", "file": path.name}


@st.cache_data(ttl=60)
def load_manifests() -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for path in sorted(MANIFEST_DIR.glob("manifest_????-??-??.json")):
        data = load_json(path)
        if not data:
            continue
        data["_file"] = path.name
        manifests.append(data)
    return manifests


@st.cache_data(ttl=60)
def load_quality_reports() -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    for path in sorted(QUALITY_DIR.glob("quality_????-??-??.json")):
        data = load_json(path)
        if not data:
            continue
        date = path.stem.replace("quality_", "")
        reports[date] = data
    return reports


def bytes_to_gb(value: int | float | None) -> float:
    return round(float(value or 0) / (1024**3), 3)


def short_hash(value: str | None) -> str:
    return value[:16] if value else "n/a"


def status_badge(status: str | None) -> str:
    if not status:
        return "n/a"
    if status in {"OK", "PATCH_OK"}:
        return f"GOOD: {status}"
    if status in {"SEGMENTED", "PATCH_PARTIAL"}:
        return f"WATCH: {status}"
    return f"CHECK: {status}"


def quality_rows(manifests: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for manifest in manifests:
        statuses = manifest.get("quality_statuses") or {}
        row = {"date": manifest.get("date")}
        for source in SOURCE_ORDER:
            row[source] = statuses.get(source, "n/a")
        row["alerts"] = len(manifest.get("alerts") or [])
        row["raw_files"] = len(manifest.get("raw_files") or [])
        row["derived_files"] = len(manifest.get("derived_files") or [])
        rows.append(row)
    return pd.DataFrame(rows)


def gap_rows(quality_reports: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for date, report in sorted(quality_reports.items()):
        for source, data in (report.get("sources") or {}).items():
            thresholds = ((data.get("temporal_gaps") or {}).get("thresholds") or {})
            ge2 = thresholds.get("2") or {}
            seq = data.get("sequence_gaps")
            rows.append(
                {
                    "date": date,
                    "source": source,
                    "status": data.get("status"),
                    "updates": (data.get("totals") or {}).get("updates"),
                    "snapshots": (data.get("totals") or {}).get("snapshots"),
                    "json_errors": (data.get("totals") or {}).get("json_errors"),
                    "gaps_ge_2s": ge2.get("count", 0),
                    "gap_seconds_ge_2s": round(float(ge2.get("total_seconds") or 0), 6),
                    "max_gap_seconds": round(float(ge2.get("max_seconds") or 0), 6),
                    "sequence_gaps": seq.get("count") if isinstance(seq, dict) else None,
                }
            )
    return pd.DataFrame(rows)


def patch_rows(manifests: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for manifest in manifests:
        for item in manifest.get("patched_qc") or []:
            name = item.get("name") or ""
            exchange = "binance" if name.startswith("binance") else "coinbase" if name.startswith("coinbase") else "unknown"
            rows.append(
                {
                    "date": manifest.get("date"),
                    "exchange": exchange,
                    "status": item.get("status"),
                    "records": item.get("records"),
                    "backup_patch_records": item.get("backup_patch_records"),
                    "sequence_gaps": item.get("sequence_gaps"),
                    "temporal_gaps_ge_2s": item.get("temporal_gaps_ge_2s"),
                    "report": name,
                }
            )
    return pd.DataFrame(rows)


def alert_rows(manifests: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for manifest in manifests:
        for alert in manifest.get("alerts") or []:
            rows.append(
                {
                    "date": manifest.get("date"),
                    "severity": alert.get("severity"),
                    "kind": alert.get("kind"),
                    "message": alert.get("message"),
                }
            )
    return pd.DataFrame(rows)


def pipeline_rows(manifests: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for manifest in manifests:
        pipeline = manifest.get("pipeline") or {}
        config = pipeline.get("config") or {}
        config_data = pipeline.get("config_data") or {}
        git = pipeline.get("git") or {}
        scripts = pipeline.get("scripts") or []
        missing = sum(1 for item in scripts if item.get("status") == "missing")
        rows.append(
            {
                "date": manifest.get("date"),
                "config_version": config_data.get("pipeline_version"),
                "config_sha256": short_hash(config.get("sha256")),
                "scripts_tracked": len(scripts),
                "missing_scripts": missing,
                "git_available": git.get("available"),
                "git_commit": short_hash(git.get("commit")),
            }
        )
    return pd.DataFrame(rows)


def raw_growth_rows(manifests: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for manifest in manifests:
        by_source: dict[str, int] = {}
        for item in manifest.get("raw_files") or []:
            role = item.get("role") or ""
            parts = role.split(":")
            source = parts[1] if len(parts) >= 2 else "unknown"
            by_source[source] = by_source.get(source, 0) + int(item.get("bytes") or 0)
        for source, size in by_source.items():
            rows.append({"date": manifest.get("date"), "source": source, "raw_gb": bytes_to_gb(size)})
    return pd.DataFrame(rows)


def status_score(status: str | None) -> int:
    return STATUS_ORDER.get(status or "", 9)


manifests = load_manifests()
quality_reports = load_quality_reports()

st.title("LOB Data Capture Monitor")
st.caption("Public audit view for BTC LOB capture. Raw data and server logs are not exposed.")

if not manifests:
    st.error("No daily manifests are available yet.")
    st.stop()

latest = manifests[-1]
qdf = quality_rows(manifests)
gdf = gap_rows(quality_reports)
pdf = patch_rows(manifests)
adf = alert_rows(manifests)
vdf = pipeline_rows(manifests)
rdf = raw_growth_rows(manifests)

latest_date = latest.get("date")
latest_alerts = latest.get("alerts") or []
latest_statuses = latest.get("quality_statuses") or {}
raw_gb_total = bytes_to_gb(sum(int(item.get("bytes") or 0) for m in manifests for item in (m.get("raw_files") or [])))
derived_gb_total = bytes_to_gb(sum(int(item.get("bytes") or 0) for m in manifests for item in (m.get("derived_files") or [])))

overall_score = max((status_score(s) for s in latest_statuses.values()), default=9)
overall = "OK" if overall_score == 0 else "Watch" if overall_score <= 1 else "Check"

metric_cols = st.columns(5)
metric_cols[0].metric("Days Audited", len(manifests))
metric_cols[1].metric("Latest Day", latest_date)
metric_cols[2].metric("Overall", overall)
metric_cols[3].metric("Raw in Manifests", f"{raw_gb_total:.2f} GB")
metric_cols[4].metric("Derived in Manifests", f"{derived_gb_total:.2f} GB")

st.subheader("Latest Source Status")
cols = st.columns(4)
for idx, source in enumerate(SOURCE_ORDER):
    cols[idx].metric(source, status_badge(latest_statuses.get(source)))

if latest_alerts:
    st.subheader("Latest Alerts")
    st.dataframe(pd.DataFrame(latest_alerts)[["severity", "kind", "message"]], use_container_width=True, hide_index=True)

tab_overview, tab_gaps, tab_patches, tab_integrity = st.tabs(
    ["Daily Quality", "Gaps", "Patches", "Integrity"]
)

with tab_overview:
    st.markdown("Daily status produced from quality reports and manifests.")
    st.dataframe(qdf, use_container_width=True, hide_index=True)
    if not rdf.empty:
        pivot = rdf.pivot_table(index="date", columns="source", values="raw_gb", aggfunc="sum").fillna(0)
        st.bar_chart(pivot)

with tab_gaps:
    st.markdown("Temporal gaps by day/source. Binance also reports official sequence gaps when available.")
    if gdf.empty:
        st.info("No gap data available.")
    else:
        st.dataframe(gdf, use_container_width=True, hide_index=True)
        gap_chart = gdf.pivot_table(index="date", columns="source", values="gap_seconds_ge_2s", aggfunc="sum").fillna(0)
        st.bar_chart(gap_chart)

with tab_patches:
    st.markdown("Conservative gap-patch outputs. PATCH_PARTIAL means backup helped but some gap remains documented.")
    if pdf.empty:
        st.info("No patch reports available yet.")
    else:
        st.dataframe(pdf, use_container_width=True, hide_index=True)
        patch_chart = pdf.pivot_table(index="date", columns="status", values="report", aggfunc="count").fillna(0)
        st.bar_chart(patch_chart)

with tab_integrity:
    st.markdown("Pipeline versioning and reproducibility metadata.")
    st.dataframe(vdf, use_container_width=True, hide_index=True)
    newest_pipeline = (latest.get("pipeline") or {})
    scripts = newest_pipeline.get("scripts") or []
    if scripts:
        script_view = pd.DataFrame([
            {
                "script": Path(item.get("path", "")).name,
                "status": item.get("status"),
                "sha256": short_hash(item.get("sha256")),
            }
            for item in scripts
        ])
        st.dataframe(script_view, use_container_width=True, hide_index=True)

st.divider()
generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
st.caption(f"Dashboard refreshed from audit artifacts at {generated_at}.")
