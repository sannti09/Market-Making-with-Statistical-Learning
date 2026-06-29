# Gap Policy

This document defines how capture gaps are interpreted in the beta pipeline.

## Raw Data

Raw files are never edited in place. If a stream disconnects, reconnects, or
misses an interval, the original capture remains as evidence.

## Primary and Backup

Each exchange can run two independent streams:

- primary
- backup

The backup stream is not merged blindly. It is used only to fill intervals where
the primary stream has an observed gap.

## Binance

For Binance, gap detection uses the official depth-update sequence fields:

- `U`: first update ID in the event
- `u`: final update ID in the event

A continuous stream should satisfy:

```text
next U == previous u + 1
```

If a primary sequence range is missing and the backup contains the complete
range, the gap can be patched with higher confidence.

## Coinbase

For Coinbase Exchange `level2_batch`, the beta pipeline does not assume
Binance-style official sequence continuity. Gaps are identified using message
timestamps, reconnect markers, and snapshots.

When backup messages fall inside a primary gap, they can be inserted into a
derived patched file. The resulting continuity is described as observational
timestamp continuity, not official sequence continuity.

## Modeling Use

Recommended usage:

- Use `OK` and `PATCH_OK` intervals first.
- Use `SEGMENTED` intervals only with explicit segment flags.
- Exclude rows around unresolved gaps from model training and evaluation unless
  the experiment is explicitly about robustness to missing data.
