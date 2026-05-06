# Private Analyzer Mode

FantaPortoscuso keeps `manual_import` as the default production-safe mode.
For private/personal league analysis, you can explicitly enable the legacy remote workflow.

## Safe Default

Default mode stays manual-import-first:

```env
PRODUCT_MODE=manual_import
DATA_IMPORT_MODE=manual
ENABLE_LEGACY_REMOTE_IMPORTS=false
ENABLE_MANUAL_IMPORTS=true
```

With this setup, legacy remote jobs and legacy admin endpoints remain blocked.

## Private Mode

To enable the private analyzer legacy remote mode, set:

```env
PRODUCT_MODE=private_analyzer
DATA_IMPORT_MODE=legacy_remote
ENABLE_LEGACY_REMOTE_IMPORTS=true
ENABLE_MANUAL_IMPORTS=true
AUTO_INTERNAL_SCHEDULERS_ENABLED=true
AUTO_LIVE_IMPORT_ENABLED=true
AUTO_SERIEA_LIVE_SYNC_ENABLED=true
AUTO_LEGHE_SYNC_ENABLED=true
AUTO_LEGHE_SYNC_ON_START=true
```

Optional legacy credentials/config:

```env
LEGHE_ALIAS=
LEGHE_USERNAME=
LEGHE_PASSWORD=
LEGHE_COMPETITION_ID=
LEGHE_COMPETITION_NAME=
LEGHE_FORMATIONS_MATCHDAY=
```

## Notes

- The default remains manual-import-first.
- This mode is intended for private/personal league usage.
- Keep credentials only in local `.env`, Railway variables, or other secret managers.
- Never commit real credentials to the repository.
