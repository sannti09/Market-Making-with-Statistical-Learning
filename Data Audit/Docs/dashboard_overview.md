# Public Dashboard Overview

The public dashboard is intentionally limited to lightweight audit artifacts.
It is designed to show the health of the data pipeline without exposing raw
market data or private server details.

## Public Views

- latest audited day
- source status by exchange
- daily quality table
- temporal gaps by source/day
- gap patch results
- pipeline config version
- script/config hashes
- recent alerts

## Not Public

- raw JSONL files
- full logs
- private keys
- SSH commands
- server credentials
- large derived datasets

## Deployment

The beta deployment uses Streamlit under systemd:

```text
lob-dashboard-public.service
```

The first public version can run on port `8501`. A more polished deployment
should use a domain, HTTPS, and a reverse proxy such as Nginx.
