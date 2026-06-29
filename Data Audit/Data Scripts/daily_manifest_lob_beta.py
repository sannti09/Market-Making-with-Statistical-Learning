#!/usr/bin/env python3
"""
Daily manifest builder for the LOB capture system.

Creates an auditable manifest with SHA256 hashes for raw inputs, derived patch
outputs, quality reports, and a short human-readable summary. It does not modify
raw captures.
"""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


RAW_LAYOUT = [
    ("binance", "BTCUSDT"),
    ("binance_backup", "BTCUSDT"),
    ("coinbase", "BTC-USD"),
    ("coinbase_backup", "BTC-USD"),
]

SCRIPT_VERSION_TARGETS = [
    "live_lob_capture.py",
    "quality_report_lob_beta.py",
    "run_daily_quality_reports_beta.py",
    "merge_binance_gap_patch_beta.py",
    "merge_coinbase_gap_patch_beta.py",
    "run_daily_gap_patches_beta.py",
    "daily_manifest_lob_beta.py",
    "run_daily_manifests_beta.py",
    "build_coinbase_lob_beta.py",
]


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def file_entry(path: Path, role: str, sha256: bool) -> Dict:
    entry = {
        "role": role,
        "path": str(path),
        "bytes": path.stat().st_size,
        "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if sha256:
        entry["sha256"] = sha256_file(path)
    return entry


def optional_file_entry(path: Path, role: str, sha256: bool) -> Dict:
    if not path.exists():
        return {
            "role": role,
            "path": str(path),
            "status": "missing",
        }
    entry = file_entry(path, role, sha256)
    entry["status"] = "present"
    return entry


def git_metadata(root: Path) -> Dict:
    try:
        commit = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            text=True,
            capture_output=True,
            check=False,
        )
        dirty = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain"],
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        return {"available": False, "reason": str(exc)}

    if commit.returncode != 0:
        return {
            "available": False,
            "reason": (commit.stderr or commit.stdout).strip()[-500:],
        }
    return {
        "available": True,
        "commit": commit.stdout.strip(),
        "dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else None,
    }


def collect_pipeline_versions(root: Path, config_path: Path, with_hash: bool) -> Dict:
    scripts_dir = root / "scripts"
    scripts = [
        optional_file_entry(scripts_dir / name, "pipeline_script", with_hash)
        for name in SCRIPT_VERSION_TARGETS
    ]
    config_entry = optional_file_entry(config_path, "pipeline_config", with_hash)
    config = load_json(config_path) if config_path.exists() else None
    return {
        "python": {
            "executable": sys.executable,
            "version": sys.version.split()[0],
        },
        "git": git_metadata(root),
        "config": config_entry,
        "config_data": config,
        "scripts": scripts,
    }


def collect_raw(root: Path, day: date, with_hash: bool) -> List[Dict]:
    out = []
    day_s = day.isoformat()
    for source, symbol in RAW_LAYOUT:
        for base in ("raw_done", "raw_live"):
            folder = root / base / source / symbol
            if not folder.exists():
                continue
            for path in sorted(folder.glob(f"*{day_s}*.jsonl*")):
                out.append(file_entry(path, f"{base}:{source}:{symbol}", with_hash))
    return out


def collect_derived(root: Path, day: date, with_hash: bool) -> List[Dict]:
    out = []
    day_s = day.isoformat()
    for folder, role in [
        (root / "merged_beta", "merged_beta"),
        (root / "reports" / "quality_beta", "quality_report"),
        (root / "reports" / "merge_beta", "merge_report"),
        (root / "features_beta", "features_beta"),
    ]:
        if not folder.exists():
            continue
        for path in sorted(folder.rglob(f"*{day_s}*")):
            if path.is_file():
                out.append(file_entry(path, role, with_hash))
    return out


def load_json(path: Path) -> Optional[Dict]:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def status_line(source: str, data: Optional[Dict]) -> str:
    if not data:
        return f"- {source}: no quality data"
    totals = data.get("totals") or {}
    gaps = ((data.get("temporal_gaps") or {}).get("thresholds") or {}).get("2", {})
    seq = data.get("sequence_gaps")
    seq_count = seq.get("count") if isinstance(seq, dict) else "n/a"
    return (
        f"- {source}: {data.get('status')} | updates={totals.get('updates')} "
        f"| snapshots={totals.get('snapshots')} | json_errors={totals.get('json_errors')} "
        f"| gaps>=2s={gaps.get('count')} | seq_gaps={seq_count}"
    )


PROBLEM_STATUSES = {
    "CAPTURE_GAP_UNRESOLVED",
    "DIAGNOSTIC_ONLY",
    "REJECT_CORRUPT_JSON",
    "REJECT_NO_SNAPSHOT",
    "NO_UPDATES",
}


def threshold_count(stats: Optional[Dict], threshold: str = "2") -> int:
    if not isinstance(stats, dict):
        return 0
    thresholds = stats.get("thresholds") or {}
    if threshold in thresholds:
        return int((thresholds.get(threshold) or {}).get("count") or 0)
    numeric = []
    for key in thresholds:
        try:
            numeric.append((float(key), key))
        except (TypeError, ValueError):
            pass
    for _, key in sorted(numeric):
        if float(key) >= float(threshold):
            return int((thresholds.get(key) or {}).get("count") or 0)
    return 0


def classify_patch_report(path: Path, report: Dict) -> Dict:
    name = path.name
    merged = report.get("merged") or {}
    patch = report.get("patch") or {}
    backup = report.get("backup") or {}
    output = merged.get("output")

    seq_count = None
    if isinstance(merged.get("sequence_gaps"), dict):
        seq_count = int(merged["sequence_gaps"].get("count") or 0)

    gap_stats = merged.get("gap_stats") or merged.get("temporal_gaps")
    temporal_ge_2s = threshold_count(gap_stats, "2")
    patch_records = int(merged.get("backup_patch_records") or patch.get("backup_patch_records") or 0)

    unresolved = bool(seq_count and seq_count > 0) or temporal_ge_2s > 0
    if unresolved and patch_records > 0:
        status = "PATCH_PARTIAL"
    elif unresolved:
        status = "PATCH_FAILED_OR_UNRESOLVED"
    else:
        status = "PATCH_OK"

    return {
        "report": str(path),
        "name": name,
        "status": status,
        "output": output,
        "records": merged.get("records"),
        "primary_records": merged.get("primary_records"),
        "backup_patch_records": patch_records,
        "patch_l2updates": merged.get("patch_l2updates"),
        "backup_inside_gaps": backup.get("l2updates_inside_primary_gaps"),
        "sequence_gaps": seq_count,
        "temporal_gaps_ge_2s": temporal_ge_2s,
    }


def build_alerts(quality: Optional[Dict], patch_qc: List[Dict], raw_files: List[Dict], derived_files: List[Dict], pipeline: Dict) -> List[Dict]:
    alerts = []
    sources = (quality or {}).get("sources") or {}
    for source, data in sources.items():
        status = data.get("status")
        totals = data.get("totals") or {}
        if status in PROBLEM_STATUSES:
            alerts.append({
                "severity": "error",
                "kind": "quality_status",
                "source": source,
                "status": status,
                "message": f"{source} quality status is {status}",
            })
        elif status == "SEGMENTED":
            alerts.append({
                "severity": "warning",
                "kind": "quality_status",
                "source": source,
                "status": status,
                "message": f"{source} has observed capture segments or temporal gaps",
            })
        if int(totals.get("json_errors") or 0) > 0:
            alerts.append({
                "severity": "error",
                "kind": "json_errors",
                "source": source,
                "count": totals.get("json_errors"),
                "message": f"{source} has JSON parse errors",
            })

    for item in patch_qc:
        if item["status"] != "PATCH_OK":
            severity = "warning" if item["status"] == "PATCH_PARTIAL" else "error"
            alerts.append({
                "severity": severity,
                "kind": "patched_quality",
                "report": item["report"],
                "status": item["status"],
                "sequence_gaps": item["sequence_gaps"],
                "temporal_gaps_ge_2s": item["temporal_gaps_ge_2s"],
                "message": f"{item['name']} ended as {item['status']}",
            })

    if not raw_files:
        alerts.append({
            "severity": "error",
            "kind": "missing_raw_files",
            "message": "No raw input files were found for this date",
        })
    if not derived_files:
        alerts.append({
            "severity": "warning",
            "kind": "missing_derived_files",
            "message": "No derived files were found for this date",
        })
    if (pipeline.get("config") or {}).get("status") == "missing":
        alerts.append({
            "severity": "warning",
            "kind": "missing_pipeline_config",
            "message": "Pipeline config file is missing",
        })
    for script in pipeline.get("scripts") or []:
        if script.get("status") == "missing":
            alerts.append({
                "severity": "warning",
                "kind": "missing_pipeline_script",
                "path": script.get("path"),
                "message": f"Pipeline script is missing: {script.get('path')}",
            })
    return alerts


def build_human_summary(day: date, quality: Optional[Dict], merge_reports: List[Path], patch_qc: List[Dict], alerts: List[Dict], pipeline: Dict) -> str:
    lines = [
        f"# LOB Daily Data Summary - {day.isoformat()}",
        "",
        "## Quality",
    ]
    sources = (quality or {}).get("sources") or {}
    for source in ["binance", "binance_backup", "coinbase", "coinbase_backup"]:
        lines.append(status_line(source, sources.get(source)))

    lines.extend(["", "## Gap Patch Reports"])
    if not merge_reports:
        lines.append("- none")
    patch_by_report = {item["report"]: item for item in patch_qc}
    for path in merge_reports:
        item = patch_by_report.get(str(path)) or {}
        lines.append(
            f"- {path.name}: {item.get('status')} | records={item.get('records')} "
            f"| patch_records={item.get('backup_patch_records')} "
            f"| patch_l2updates={item.get('patch_l2updates')} "
            f"| backup_inside_gaps={item.get('backup_inside_gaps')} "
            f"| seq_gaps={item.get('sequence_gaps')} "
            f"| gaps>=2s={item.get('temporal_gaps_ge_2s')}"
        )

    lines.extend(["", "## Alerts"])
    if not alerts:
        lines.append("- none")
    for alert in alerts:
        lines.append(f"- {alert.get('severity')}: {alert.get('message')}")

    lines.extend(["", "## Pipeline Version"])
    config = pipeline.get("config") or {}
    config_data = pipeline.get("config_data") or {}
    git = pipeline.get("git") or {}
    lines.append(f"- config: {config.get('path')} | status={config.get('status')} | sha256={config.get('sha256')}")
    lines.append(f"- config_version: {config_data.get('pipeline_version')}")
    lines.append(f"- git_available: {git.get('available')} | commit={git.get('commit')} | dirty={git.get('dirty')}")
    missing_scripts = [s for s in pipeline.get("scripts") or [] if s.get("status") == "missing"]
    lines.append(f"- scripts_tracked: {len(pipeline.get('scripts') or [])} | missing_scripts={len(missing_scripts)}")

    lines.extend(["", "## Interpretation"])
    lines.append("- Raw files are preserved. Derived files are written separately.")
    lines.append("- PATCH_OK means the patched output has no remaining detected gap under the current checks.")
    lines.append("- PATCH_PARTIAL means backup helped but some gap remains documented.")
    lines.append("- SEGMENTED means usable by segment or after conservative backup patching.")
    lines.append("- CAPTURE_GAP_UNRESOLVED means the capture did not observe a required continuity interval.")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/lob_system")
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-dir", default="/opt/lob_system/manifests")
    parser.add_argument("--config", default="/opt/lob_system/config/pipeline_config.json")
    parser.add_argument("--no-hash", action="store_true", help="Skip SHA256 calculation")
    args = parser.parse_args()

    root = Path(args.root)
    day = parse_date(args.date)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with_hash = not args.no_hash
    config_path = Path(args.config)

    raw_files = collect_raw(root, day, with_hash)
    derived_files = collect_derived(root, day, with_hash)
    pipeline = collect_pipeline_versions(root, config_path, with_hash)
    quality_path = root / "reports" / "quality_beta" / f"quality_{day.isoformat()}.json"
    quality = load_json(quality_path)
    merge_report_dir = root / "reports" / "merge_beta"
    all_merge_reports = sorted(merge_report_dir.glob(f"*{day.isoformat()}*_report.json")) if merge_report_dir.exists() else []
    patch_reports = [path for path in all_merge_reports if "gap_patch_report" in path.name]
    patch_qc = [classify_patch_report(path, load_json(path) or {}) for path in patch_reports]
    alerts = build_alerts(quality, patch_qc, raw_files, derived_files, pipeline)

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "date": day.isoformat(),
        "root": str(root),
        "hash_algorithm": "sha256" if with_hash else None,
        "pipeline": pipeline,
        "raw_files": raw_files,
        "derived_files": derived_files,
        "quality_report": str(quality_path) if quality_path.exists() else None,
        "quality_statuses": {
            source: data.get("status")
            for source, data in ((quality or {}).get("sources") or {}).items()
        },
        "merge_reports": [str(p) for p in all_merge_reports],
        "gap_patch_reports": [str(p) for p in patch_reports],
        "patched_qc": patch_qc,
        "alerts": alerts,
    }

    manifest_path = out_dir / f"manifest_{day.isoformat()}.json"
    summary_path = out_dir / f"summary_{day.isoformat()}.md"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(build_human_summary(day, quality, patch_reports, patch_qc, alerts, pipeline))

    print(json.dumps({"manifest": str(manifest_path), "summary": str(summary_path)}, indent=2))


if __name__ == "__main__":
    main()
