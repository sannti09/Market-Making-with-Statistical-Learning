# Real-Time LOB Capture and Audit Pipeline

This repository contains a beta data-engineering pipeline for live crypto limit
order book (LOB) research. It captures WebSocket market data, runs daily quality
control, applies conservative backup-based gap patches, creates reproducibility
manifests, and serves a public audit dashboard.

The project is designed for market microstructure research, where imperfect live
data is expected and must be documented rather than hidden.

## Scope

Current instruments:

- Binance `BTCUSDT` depth stream at `100ms`
- Coinbase Exchange `BTC-USD` `level2_batch`
- Primary and backup captures for each exchange

The repository contains code and configuration examples only. Raw market data is
not included.

## Architecture

```text
WebSocket captures
  -> raw_live/
  -> raw_done/ compressed rotations
  -> daily QC reports
  -> conservative gap patch outputs
  -> daily manifests with SHA256 hashes
  -> public dashboard from lightweight audit artifacts
```

## Key Features

- Live WebSocket capture for Binance and Coinbase Exchange
- Independent primary and backup streams
- 12-hour raw file rotation and compression
- Daily quality reports
- Binance sequence-gap checks using official `U/u` fields
- Coinbase timestamp/snapshot/segment checks
- Conservative backup patching into derived `.jsonl.gz` outputs
- SHA256 hashes for raw inputs, derived outputs, scripts, and config
- Daily human-readable summaries
- Public Streamlit dashboard for audit visibility

## Repository Layout

```text
scripts/            Capture, QC, gap patch, manifest, and beta LOB scripts
dashboard_public/   Public Streamlit dashboard
config/             Example pipeline configuration
systemd/            Example Linux service and timer units
docs/               Methodology notes
```

## What Is Not Included

The following should not be committed:

- raw `.jsonl` / `.jsonl.gz` market data
- derived large LOB datasets
- private keys
- server logs
- machine-specific secrets
- real private infrastructure credentials

See `.gitignore`.

## Current Methodological Position

Binance continuity can be audited with official sequence IDs. Coinbase Exchange
`level2_batch` does not provide the same Binance-style sequence continuity in
the captured messages, so Coinbase continuity is documented through timestamps,
snapshots, segment markers, and backup-patch reports.

This means Binance patch quality is stronger in a formal sequence sense, while
Coinbase remains usable when the audit trail clearly identifies gaps, patches,
and residual limitations.

## Running the Dashboard

Install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Run locally:

```bash
streamlit run dashboard_public/app.py
```

The dashboard expects audit artifacts under `/opt/lob_system` by default.
Adapt paths in `dashboard_public/app.py` for local demos.

## Status

This is a beta research infrastructure project. It is suitable for portfolio,
methodology demonstration, and academic experimentation, but not a production
trading system.
