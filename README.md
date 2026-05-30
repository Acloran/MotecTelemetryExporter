# MotecTelemetryExporter

Standalone local tool for turning Orion live-telemetry database ranges into MoTeC-compatible exports. This repo is intentionally separate from `lhre-2026` and `MotecLogGenerator`.

## Safety Rules

- Never writes to the telemetry database.
- Never modifies `lhre-2026` or `MotecLogGenerator`.
- Uses local JSON for reusable GPS track splits.
- Uses local JSON for reusable channel charts.
- Writes generated artifacts only under local ignored folders such as `exports/` and `.cache/`.
- Database code runs read-only transactions and rejects non-`SELECT` SQL.

## Current Status

This first build runs in `sample` mode without credentials. It includes:

- Orion channel registry based on the telemetry repo schema and dashboards.
- Calendar, session, channel, series, GPS, track JSON, channel chart JSON, split, and export APIs.
- A React operator UI with historical exporter, Kafka live viewer, GPS track builder, race ops brief, calendar/session picker, plot range selection, GPS preview, and export button.
- A separate Kafka replay simulator web app for publishing bounded historical sessions as live Kafka samples during testing.
- A minimal MoTeC `.ld`/`.ldx` writer implemented locally in this repo.

When credentials arrive, set `TELEMETRY_SOURCE=postgres` in `.env`.

See [docs/PLAN.md](docs/PLAN.md) for the safety contract, integration path, and next implementation phases.

## Local Setup

Backend:

```bash
cd /Users/acloran/Documents/GitHub/MotecTelemetryExporter
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
uvicorn motec_exporter.app:app --reload --app-dir backend/src --port 8010
```

Frontend:

```bash
cd /Users/acloran/Documents/GitHub/MotecTelemetryExporter/frontend
npm install
npm run dev -- --host 0.0.0.0 --port 5174
```

Open `http://localhost:5174`.

Replay simulator:

```bash
cd /Users/acloran/Documents/GitHub/MotecTelemetryExporter/simulator
npm install
npm run dev -- --host 0.0.0.0 --port 5175
```

Or from the repo root after dependencies are installed:

```bash
npm run simulator
```

Open `http://localhost:5175`. The simulator reads historical ranges through the same read-only backend APIs and only publishes to Kafka after pressing Start.

## Database Credentials

Use a read-only Orion user when available:

```env
ORION_DB_HOST=192.168.1.109
ORION_DB_PORT=5432
ORION_DB_NAME=orion
ORION_DB_USER=analysis
ORION_DB_PASSWORD=...
ORION_DB_SSLMODE=disable
TELEMETRY_SOURCE=postgres
```

The implementation follows PostgreSQL's documented read-only transaction mode and information schema discovery patterns. It does not use migrations, Prisma, `INSERT`, `UPDATE`, `DELETE`, or DDL.

## Live Viewer

The live tab consumes a topic through the local backend SSE endpoint. For local simulator testing, leave `KAFKA_MODE=local`; the simulator publishes to an in-process local topic bus and no external Kafka broker is needed. Use the same topic in the simulator and the main app live tab, such as `grafana_data_orion`.

For a real Kafka broker, set `KAFKA_MODE=kafka` and configure:

```env
KAFKA_MODE=kafka
KAFKA_BOOTSTRAP_SERVERS=192.168.1.109:29092
KAFKA_TOPIC_PREFIX=grafana_data
```

Install/update the backend environment after pulling dependency changes:

```bash
source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Kafka Replay Simulator

The simulator runs separately from the main UI and publishes historical telemetry to the same topic shape used by the live viewer. In local mode it stays entirely inside this backend process; in Kafka mode it publishes to the configured broker. It clamps replay payloads to bounded channels and frames, keeps GPS and speed selected, defaults to core telemetry channels, and uses the selected threshold session as the replay source. It never writes to PostgreSQL.
