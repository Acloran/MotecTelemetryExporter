# MoTeC Telemetry Exporter Plan

## Goal

Build a standalone local tool that reads Orion telemetry from PostgreSQL, previews fast downsampled plots, lets the operator pick calendar days, sessions, automatic split files, or manual chart ranges, and exports MoTeC-compatible `.ld`/`.ldx` packages.

This repo intentionally has no runtime dependency on `lhre-2026` or `MotecLogGenerator`. Those projects were used only as references for schema shape, channel naming, dashboard behavior, and MoTeC file structure.

## Read-Only Database Contract

- Use a dedicated read-only database user.
- Open connections with `default_transaction_read_only=on`.
- Wrap all database work in explicit `read_only=True` transactions.
- Reject any query that is not `SELECT`/CTE-shaped before it reaches PostgreSQL.
- Never run migrations, DDL, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY FROM`, `TRUNCATE`, `VACUUM`, grants, or schema changes.
- Store generated exports, cache files, and reusable track splits locally only.

Reference docs:

- PostgreSQL `SET TRANSACTION` read-only mode: https://www.postgresql.org/docs/current/sql-set-transaction.html
- PostgreSQL `COPY TO STDOUT` behavior, if streaming exports become useful later: https://www.postgresql.org/docs/current/sql-copy.html
- PostgreSQL information schema, if runtime schema validation is added: https://www.postgresql.org/docs/current/information-schema.html

## Current Build

Phase 1 is implemented in sample mode:

- FastAPI backend with calendar, day, session, series, GPS, track JSON, and export APIs.
- React/Vite frontend with day list, session list, auto-file list, selectable telemetry plot, GPS trace, and track split editor.
- Local JSON track split persistence under `tracks/`.
- Local export generation under `exports/`.
- A local MoTeC `.ld`/`.ldx` writer based on the old generator's output structure, with the old app stripped away.
- Tests for split detection, database query guardrails, track JSON, and export generation.

## Database Integration Path

When credentials are ready:

1. Copy `.env.example` to `.env`.
2. Set `TELEMETRY_SOURCE=postgres`.
3. Set `ORION_DB_HOST=192.168.1.109`, `ORION_DB_PORT=5432`, `ORION_DB_NAME=orion`, and the real read-only username/password.
4. Keep `ORION_DB_SSLMODE=disable` unless the server is reconfigured for SSL.
5. Run the backend health endpoint and calendar endpoint before opening the full UI.

The first Postgres checks should be:

```bash
curl http://127.0.0.1:8010/api/health
curl http://127.0.0.1:8010/api/calendar
```

## Near-Term Work

- Validate the generated `.ld`/`.ldx` pair in MoTeC i2 with a real Orion data slice.
- Add runtime schema diagnostics that report missing expected tables/columns without writing anything.
- Add richer GPS split editing on the map: drag gate endpoints, rename gates, and compute split crossings from GPS.
- Add channel presets for driver, powertrain, accumulator, diagnostics, and thermal exports.
- Add export progress for very large ranges.
- Add optional PostgreSQL cursor streaming for long exports if full-day pulls are too memory-heavy.

