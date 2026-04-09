# Ride Bot

Production-minded Node.js WhatsApp ride-ingestion service.

It listens to allowed WhatsApp groups, extracts deterministic ride fields locally, uses OpenAI only for cleanup/normalization, geocodes pickup/dropoff, uses OSRM for route distance/duration, calculates fare in code (unless fare is present in message), and appends the fixed 10-column row to Google Sheets.

## 1) Purpose

- Ingest ride requests from WhatsApp groups.
- Keep extraction deterministic first, AI second.
- Fail safely on external service issues.
- Persist rows to Google Sheets in strict schema order.
- Keep service stable across restarts with persistent WhatsApp login.

## 2) Architecture Flow

1. WhatsApp event received (`message` event from `whatsapp-web.js`).
2. Skip non-group/system/broadcast/personal messages.
3. Enforce `ALLOWED_GROUPS` allowlist.
4. Dedupe check (message id first, hash fallback).
5. Normalize raw text.
6. Local deterministic extraction.
7. OpenAI normalization (cleanup only).
8. Safe merge local + AI values.
9. Ensure `refer` (`RID-YYYYMMDD-XXXX`).
10. Geocode pickup + dropoff.
11. OSRM route lookup if both coordinates exist.
12. Set distance text from OSRM result.
13. Fare calculation:
   - preserve extracted fare when present
   - otherwise compute from distance/base/per-km.
14. Build final row in fixed order.
15. Append to Google Sheets with retries.
16. Mark dedupe processed only after successful append.

## 3) WhatsApp Startup Reliability

The client now uses a startup state machine with explicit states:

- `init_started`
- `persisted_session_detected`
- `qr_required`
- `authenticated`
- `ready`
- `auth_failed`
- `startup_timeout`

Behavior:

- Uses stable `WHATSAPP_CLIENT_ID` + stable `WHATSAPP_SESSION_PATH`.
- Resolves session path to absolute path.
- Detects whether saved session data exists before startup.
- Waits for `ready` with timeout (`WHATSAPP_STARTUP_TIMEOUT_MS`, default 90s).
- Emits clear lifecycle logs for QR/auth/ready/disconnect/state/loading.
- No auth/session files are deleted on normal shutdown.

## 4) Environment Variables

### Required

- `WHATSAPP_CLIENT_ID`
- `WHATSAPP_SESSION_PATH` (or `WHATSAPP_SESSION_DIR` legacy alias)
- `ALLOWED_GROUPS`
- `GOOGLE_SHEETS_ID`
- `GOOGLE_SHEETS_WORKSHEET_NAME` (or `GOOGLE_SHEETS_RANGE`)
- `GOOGLE_APPLICATION_CREDENTIALS` (or `GOOGLE_CREDENTIALS_JSON` for Railway env-injected JSON)

### Optional / Recommended

- `OPENAI_API_KEY`
- `OPENAI_MODEL` (default `gpt-4.1-mini`)
- `WHATSAPP_STARTUP_TIMEOUT_MS` (default `90000`)
- `GOOGLE_SHEETS_WORKSHEET_NAME` (default `Sheet1`)
- `GOOGLE_SHEETS_RANGE` (default `<worksheet>!A:J`)
- `GOOGLE_CREDENTIALS_JSON` (raw JSON or base64 JSON; used directly in memory)
- `GEOCODING_PROVIDER` (default `nominatim`)
- `GEOCODING_BASE_URL` (default `https://nominatim.openstreetmap.org/search`)
- `GEOCODING_USER_AGENT` (default `ride-bot/1.0 (geocode)`)
- `GEOCODING_TIMEOUT_MS` (default `12000`)
- `GEOCODING_API_KEY` (unused for default nominatim)
- `OSRM_BASE_URL` (default public OSRM)
- `DEFAULT_CURRENCY` (default `PKR`)
- `FARE_BASE` (default `250`)
- `FARE_PER_KM` (default `95`)
- `DEDUPE_STORE_PATH` (default `data/dedupe-store.json`)
- `DEDUPE_TTL_MS` (default `21600000`)
- `DEDUPE_MAX_ENTRIES` (default `20000`)
- `LOG_LEVEL` (`error|warn|info|debug`)
- `LOG_MODE` (`normal|debug`)
- `NODE_ENV`

### `.env` Example

```env
NODE_ENV=production
LOG_LEVEL=info
LOG_MODE=normal

WHATSAPP_CLIENT_ID=dispatch-bot
WHATSAPP_SESSION_PATH=/data/.wwebjs_auth
WHATSAPP_STARTUP_TIMEOUT_MS=90000
ALLOWED_GROUPS=120363408968321565@g.us,120363404505482844@g.us

OPENAI_API_KEY=
OPENAI_MODEL=gpt-4.1-mini

GOOGLE_SHEETS_ID=
GOOGLE_SHEETS_WORKSHEET_NAME=Sheet1
GOOGLE_SHEETS_RANGE=Sheet1!A:J
# Option A (recommended for Railway / env-based deploys): inject JSON directly
GOOGLE_CREDENTIALS_JSON={"type":"service_account","project_id":"...","private_key_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n","client_email":"...","client_id":"...","token_uri":"https://oauth2.googleapis.com/token"}
# Option B: mounted credentials file path
GOOGLE_APPLICATION_CREDENTIALS=/data/credentials.json

GEOCODING_PROVIDER=nominatim
GEOCODING_BASE_URL=https://nominatim.openstreetmap.org/search
GEOCODING_USER_AGENT=ride-bot/1.0
GEOCODING_TIMEOUT_MS=12000
GEOCODING_API_KEY=

DEFAULT_CURRENCY=PKR
FARE_BASE=250
FARE_PER_KM=95

DEDUPE_STORE_PATH=./data/dedupe-store.json
DEDUPE_TTL_MS=21600000
DEDUPE_MAX_ENTRIES=20000
```

### Google service-account setup

1. Download your service-account key JSON from Google Cloud.
2. Preferred: set `GOOGLE_CREDENTIALS_JSON` to the raw JSON string (or base64-encoded JSON).
3. Alternative: place the file at a secure path and set `GOOGLE_APPLICATION_CREDENTIALS`.
4. Open your target spreadsheet and share it with the `client_email` value inside the service-account JSON (Editor access recommended).

Startup auth logs will clearly show:

- `Google credentials ready`
- `Google credentials missing`
- `Google credentials JSON is invalid`
- `Google credentials are missing required fields`
- `Google Sheets auth ready`

## 5) Setup and Run

1. Install Node.js 20+.
2. Install dependencies:

```bash
npm install
```

3. Configure `.env`.
4. Start service:

```bash
npm start
```

5. Development run:

```bash
npm run dev
```

## 6) QR Scan Flow

1. Start the service.
2. If no valid saved session exists, `qr_required` is logged and QR prints to terminal.
3. In WhatsApp mobile app: `Linked Devices` -> `Link a Device`.
4. After successful login, session is saved in `WHATSAPP_SESSION_PATH`.
5. On restart, service should reuse saved session and reach `ready` without QR.

## 7) Group Filtering

- Only group chats are accepted.
- `ALLOWED_GROUPS` is strict allowlist by chat id.
- If allowlist is empty, processing is disabled (safe default).
- Broadcast/status/system/personal messages are ignored.

## 8) Dedupe Behavior

- Primary dedupe key: message id.
- Fallback dedupe key: hash(raw_message + group + timestamp).
- Processed messages are written to `DEDUPE_STORE_PATH`.
- Store survives restarts and is TTL/prune controlled.
- Message is marked processed only after successful Sheets append.

## 9) OpenAI Normalization Rules

OpenAI is used only after local extraction and only for cleanup/normalization:

- returns strict schema JSON
- includes all keys
- missing values are empty strings
- must not calculate route distance
- must not invent fare

Fallback: if OpenAI fails, local extracted values are used.

## 10) Geocoding + OSRM

### Geocoding

- Cleans address before lookup:
  - removes bullets (e.g. `•`)
  - removes label prefixes (`Pick Up:`, `Pickup:`, `Pick-up:`, `Drop Off:`, `Drop-off:`)
  - trims and collapses whitespace
- Nominatim request uses:
  - `q=<cleaned address>`
  - `format=jsonv2`
  - `limit=1`
  - proper `User-Agent`

Failure tags:

- `address_empty`
- `api_empty_array`
- `request_failed`
- `invalid_coordinates`

### OSRM

- Uses driving profile via HTTP route API.
- Requests `overview=false`.
- Returns:
  - `distance_meters`
  - `duration_seconds`
  - `distance_text`
  - `duration_text`

If geocode/OSRM fails, distance stays blank and pipeline continues.

## 11) Fare Logic

- If extracted fare exists, preserve it exactly.
- Else calculate:

`total = baseFare + distanceKm * perKmRate`

- Format with configured currency symbol.
- If distance unavailable and no extracted fare: fare remains blank.

## 12) Google Sheets Append

Final row is always mapped in this exact order:

1. `refer`
2. `day_date`
3. `starting`
4. `pickup`
5. `drop_off`
6. `distance`
7. `fare`
8. `required_vehicle`
9. `expires`
10. `expires_utc`

Append behavior:

- default range derived from worksheet (`<worksheet>!A:J`)
- retries transient failures
- classifies final failures clearly:
  - `SHEETS_AUTH_FAILED`
  - `SHEETS_PERMISSION_DENIED`
  - `SHEETS_SPREADSHEET_NOT_FOUND`
  - `SHEETS_WORKSHEET_NOT_FOUND`
  - `SHEETS_NETWORK_TIMEOUT`

## 13) Failure Behavior

- Local extraction failure -> blank-safe fallback object.
- OpenAI failure -> local fallback.
- Geocoding failure -> no coordinates.
- OSRM failure -> no distance.
- Sheets append failure -> controlled error, dedupe not marked.
- Message pipeline errors are contained; service keeps running.

## 14) Logging Behavior

- `LOG_MODE=normal`: concise, human-readable operator logs.
- `LOG_MODE=debug`: includes expanded metadata/stacks.
- Sensitive values are masked:
  - API/private keys redacted
  - phone numbers masked
  - message/body logs are preview-only

## 15) Tests and Validation

Run default offline checks:

```bash
npm test
```

Run unit only:

```bash
npm run test:unit
```

Run smoke only:

```bash
npm run test:smoke
```

Integration (env-gated):

```bash
npm run test:integration
```

Enable integration pipeline test:

- Linux/macOS: `RIDE_BOT_RUN_INTEGRATION=1 npm run test:integration`
- Windows (cmd): `set RIDE_BOT_RUN_INTEGRATION=1&& npm run test:integration`

Dry-run pipeline (no WhatsApp required):

```bash
npm run dry-run -- --use-sample
```

## 16) Startup Diagnostics Runbook

If startup does not reach ready:

1. Check boot summary values:
   - allowed groups
   - session dir (absolute)
   - geocoding provider
   - sheets configured
   - openai configured
2. Check WhatsApp lifecycle logs:
   - session found vs QR required
   - authenticated
   - ready
   - startup timeout hint
3. If timeout repeats with saved session:
   - verify linked device is still active
   - verify persistent storage is stable
   - restart once after ensuring no stale browser process lock

## 17) Deployment Notes (Railway / Containers)

### Docker image

- This repo includes a Railway-friendly `Dockerfile` (`node:20-bookworm-slim`) with Chromium/Puppeteer runtime dependencies.
- Default container runtime path for WhatsApp LocalAuth is `/data/.wwebjs_auth` in production.

### Railway deployment steps

1. Push the repo to GitHub.
2. In Railway, create a new project and connect the GitHub repo.
3. Railway will detect the `Dockerfile`; deploy using Docker.
4. Add a **Persistent Volume** and mount it at `/data`.
5. Set Railway env vars:
   - `NODE_ENV=production`
   - `WHATSAPP_CLIENT_ID=dispatch-bot` (stable value, never randomize)
   - `WHATSAPP_SESSION_PATH=/data/.wwebjs_auth`
   - `GOOGLE_SHEETS_ID=...`
   - `GOOGLE_SHEETS_WORKSHEET_NAME=Master`
   - `GOOGLE_CREDENTIALS_JSON=...` (service-account JSON or base64 JSON)
   - `ALLOWED_GROUPS=...`
   - `OPENAI_API_KEY=...`
   - `GEOCODING_PROVIDER=nominatim`
6. Redeploy.
7. On first boot, scan QR once.
8. On next restarts, session is loaded from `/data/.wwebjs_auth` and QR should not be required unless session is invalid.

### First-time QR flow on Railway

1. Open Railway logs after deploy.
2. Wait for `QR required because no valid session exists`.
3. Scan the QR.
4. Confirm logs show `WhatsApp authenticated` then `WhatsApp connected`.

### Restart behavior

- Normal restart preserves LocalAuth session files on `/data`.
- Session files are not deleted by shutdown hooks.
- QR appears again only when WhatsApp session is actually invalid/expired.

## 18) Sample Message

```text
Saloon Car (1 Persons)

Landing
Tuesday 7th October 2025, 20:05 pm

Route
 - Pick Up: Heathrow Airport, Terminal 4
 - Drop Off: 12, Woodlands Close, Dibden Purlieu, Southampton, SO45 4JG

Head Passenger
Jessica Walker

Mobile Number
+447495292728

Flight
VY6652

Arriving From
Barcelona

£50
```

## 19) Sample Final Row Mapping

```js
[
  "RID-20260311-AB12",
  "Tuesday 7th October 2025",
  "20:05 pm",
  "Heathrow Airport, Terminal 4",
  "12, Woodlands Close, Dibden Purlieu, Southampton, SO45 4JG",
  "128.4 km",
  "£50",
  "Saloon Car",
  "",
  ""
]
```

## 20) Definition of Done Checklist

- [x] WhatsApp `LocalAuth` uses stable client id and persistent path.
- [x] Startup waits for ready and times out with clear cause summary.
- [x] Startup failures print clear reason/code (+ stack in development).
- [x] No vague unhandled rejection path from startup/message callbacks.
- [x] Deterministic extraction runs before OpenAI.
- [x] OpenAI used only for normalization cleanup.
- [x] Geocoding runs before OSRM and cleans addresses.
- [x] OSRM used only for route distance/duration.
- [x] Fare preserved from message when present, else calculated in code.
- [x] Google Sheet row mapping remains fixed `A:J` schema order.
- [x] Sheets append retries + classified terminal errors.
- [x] Dedupe survives restart via file-backed store.
- [x] Allowed-group filtering enforced.
- [x] Fail-safe behavior maintained across pipeline stages.
- [x] Dry-run mode available.
- [x] Unit/smoke/integration test scripts provided.
