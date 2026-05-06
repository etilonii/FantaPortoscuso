# Data Sources

FantaPortoscuso is manual-import-first. The default production mode is safe mode:
admins load data through upload/manual import, user-provided files, or authorized/licensed APIs.

## Defaults

```env
PRODUCT_MODE=manual_import
DATA_IMPORT_MODE=manual
ENABLE_LEGACY_REMOTE_IMPORTS=false
ENABLE_MANUAL_IMPORTS=true
ENABLE_LICENSED_API_IMPORTS=false
```

With these defaults, automatic legacy remote imports and scheduled remote sync jobs do not run.
Admin endpoints that depend on legacy remote imports return `403` with a clear message.

## Admin Responsibility

Data uploaded by an admin must be authorized for the league and for the intended product use.
Prefer local CSV/XLSX/manual uploads and data produced by the league users themselves.

## Legacy Remote Integrations

Some historical code paths can still reference third-party league or football data sources
through variables such as `LEGHE_*`, remote source URLs, or scheduled import jobs. They are
kept for compatibility and local migration work, but are disabled by default.

To run them in a controlled local/dev environment, set explicit environment variables:

```env
ENABLE_LEGACY_REMOTE_IMPORTS=true
AUTO_LEGHE_SYNC_ENABLED=true
AUTO_SERIEA_LIVE_SYNC_ENABLED=true
AUTO_LIVE_IMPORT_ENABLED=true
```

Only enable those integrations when the data source and credentials are authorized.
