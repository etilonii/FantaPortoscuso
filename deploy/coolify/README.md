# Deploy senza Railway: Coolify + Oracle + Vercel

Questa guida sostituisce Railway con una stack gratuita basata su:
- `Coolify` self-hosted su una VM `Oracle Cloud Always Free`
- backend FastAPI deployato dal `Dockerfile`
- frontend React/Vite pubblicato su `Vercel`

L'obiettivo e' mantenere il comportamento attuale del progetto:
- backend Dockerizzato
- storage persistente in `data/`
- SQLite locale in `data/db/app.db`
- scheduler interni del backend
- frontend separato con `VITE_API_BASE`

## Architettura consigliata
- `Vercel`: ospita solo `apps/web`
- `Coolify`: ospita solo il backend API
- `Oracle VM`: ospita Coolify e il volume persistente del backend
- dominio API consigliato: `api.tuodominio.it`

## Cosa e' stato preparato nel repo
- Il frontend non usa piu' il fallback hardcoded Railway.
- Il container API ora usa `apps/api/start.sh`.
- Il container salva i dati runtime in `/app/data`.
- All'avvio, se il volume e' vuoto o parziale, il container popola i file mancanti da uno snapshot incluso nell'immagine.

## 1. Preparare Oracle Cloud Always Free
1. Crea una VM Always Free Ubuntu o Debian.
2. Assegna un IP pubblico statico se disponibile nel tuo tenant.
3. Apri le porte `80` e `443` oltre alla `22`.
4. Accedi via SSH alla VM.

## 2. Installare Coolify
1. Installa Docker sulla VM.
2. Installa Coolify seguendo la procedura ufficiale.
3. Apri l'interfaccia web di Coolify e completa il setup iniziale.

## 3. Creare l'app backend in Coolify
In Coolify crea una nuova `Application` da repository Git.

Valori consigliati:
- Repository: questo repo
- Branch: il tuo branch di produzione
- Build pack: `Dockerfile`
- Dockerfile path: `apps/api/Dockerfile`
- Port exposed: `8001`
- Health check path: `/health`

## 4. Aggiungere storage persistente
Nel servizio backend aggiungi un volume persistente:
- mount path: `/app/data`

Questo punto e' obbligatorio. Senza volume persistente perderai:
- `data/db/app.db`
- `data/history`
- `data/reports`
- eventuali backup e file generati runtime

## 5. Variabili ambiente backend
Usa come base `deploy/coolify/api.env.example`.

Valori minimi obbligatori:
- `PORT=8001`
- `DATABASE_URL=sqlite:///./data/db/app.db`
- `AUTH_SECRET=<stringa lunga e stabile>`

Se vuoi mantenere il comportamento attuale degli scheduler:
- `AUTO_INTERNAL_SCHEDULERS_ENABLED=1`
- `AUTO_LIVE_IMPORT_ENABLED=1`
- `AUTO_SERIEA_LIVE_SYNC_ENABLED=1`

Se non vuoi task automatici all'avvio:
- `AUTO_LIVE_IMPORT_ON_START=0`
- `AUTO_SERIEA_LIVE_SYNC_ON_START=0`

## 6. Migrare i dati da Railway
Prima del cutover esporta i dati correnti da Railway, almeno:
- `data/db/app.db`
- `data/history/`
- `data/reports/`
- eventuali file aggiornati in `data/`

Poi caricali nel volume persistente del backend Coolify mantenendo la stessa struttura sotto `/app/data`.

Se non importi nulla, il container partira' comunque con lo snapshot incluso nel repository, ma potresti perdere dati piu' recenti rispetto a quelli presenti nel repo.

## 7. Dominio e HTTPS
1. Associa il dominio API in Coolify, ad esempio `api.tuodominio.it`.
2. Abilita il certificato TLS gestito da Coolify.
3. Verifica che `https://api.tuodominio.it/health` risponda correttamente.

## 8. Collegare Vercel
Nel progetto Vercel del frontend imposta:

```env
VITE_API_BASE=https://api.tuodominio.it
```

Poi redeploya il frontend.

Nota:
- in questo repo il fallback di produzione e' ora `/api`
- quindi, se preferisci, puoi anche configurare un proxy o rewrite lato Vercel
- ma il percorso piu' semplice e' impostare direttamente `VITE_API_BASE` all'host pubblico della API

## 9. Checklist finale
- `https://api.tuodominio.it/health` risponde `200`
- login frontend funzionante
- endpoint `/data/*` raggiungibili dal frontend
- volume persistente montato su `/app/data`
- `AUTH_SECRET` impostato e stabile
- scheduler configurati come desiderato

## 10. Limiti della migrazione minima
Questa configurazione mantiene SQLite per ridurre il rischio e replicare il comportamento attuale.

Se in futuro vuoi una piattaforma piu' robusta:
- puoi migrare il backend a PostgreSQL
- puoi usare Coolify per gestire anche Postgres
- il frontend Vercel non richiede cambiamenti strutturali ulteriori
