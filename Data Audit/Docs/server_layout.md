# Server Layout

The live server layout used during beta development is:

```text
/opt/lob_system/
  config/
  dashboard_public/
  features_beta/
  logs/
  manifests/
  merged_beta/
  raw_done/
  raw_live/
  reports/
  scripts/
  venv/
```

Only code, configuration examples, and methodology documents belong in this
repository. Runtime folders such as `raw_live`, `raw_done`, `merged_beta`,
`features_beta`, `reports`, and `manifests` are excluded from Git by default.

## Daily Jobs

The beta system uses three daily timers:

```text
lob-quality-daily.timer
lob-gap-patch-daily.timer
lob-manifest-daily.timer
```

The intended order is:

```text
quality report -> gap patch -> manifest and human summary
```
