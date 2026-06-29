# Data Audit Methodology

The pipeline treats live LOB data as an observable stream that can fail,
disconnect, reconnect, or temporarily lose continuity. The objective is not to
pretend the stream is perfect, but to record what happened and decide which
segments are usable.

## Principles

- Raw capture files are immutable.
- Derived files are written separately.
- Primary and backup streams are audited independently.
- Gap patching is conservative and documented.
- Every daily manifest records SHA256 hashes for inputs, outputs, scripts, and
  config.
- A day can be useful even if it is segmented, as long as the affected intervals
  are identified and excluded or patched.

## Statuses

- `OK`: no detected JSON errors, no unresolved sequence gaps, and no temporal
  gaps above the configured threshold.
- `SEGMENTED`: the stream contains temporal segments or reconnects. It may still
  be usable by segment or after backup patching.
- `CAPTURE_GAP_UNRESOLVED`: a required continuity interval was not observed.
- `DIAGNOSTIC_ONLY`: useful for inspection but not suitable as a final modeling
  input.
- `REJECT_CORRUPT_JSON`: JSON parsing errors were detected.
- `REJECT_NO_SNAPSHOT`: required snapshot state is missing.
- `NO_UPDATES`: no usable order book updates were found.

## Patched Statuses

- `PATCH_OK`: after conservative backup patching, no remaining gap is detected
  under the current checks.
- `PATCH_PARTIAL`: backup added messages, but at least one gap remains.
- `PATCH_FAILED_OR_UNRESOLVED`: patching did not resolve the detected gap.

## Exchange-Specific Notes

Binance provides sequence fields (`U` and `u`) in depth updates. This allows a
strong continuity check based on official update IDs.

Coinbase Exchange `level2_batch` is audited through timestamps, snapshots,
segments, and backup coverage. This is useful, but it is not the same as an
official sequence-ID proof.
