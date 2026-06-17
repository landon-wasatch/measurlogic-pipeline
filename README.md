# MeasureLogic Daily Ingest Pipeline

Pulls yesterday's data from the FieldPop/MeasureLogic API and upserts into PostgreSQL on Railway.

## Tables Created

### `measurelogic_interval`
Raw per-timestamp readings for each device/child/point combination.

| Column | Description |
|---|---|
| `date` | Calendar date of the reading |
| `timestamp_utc` | Exact UTC timestamp of the datapoint |
| `device_id` | FieldPop device ID |
| `child_id` | Child device/meter ID |
| `point_name` | Data point (e.g. `EnergyP_Tot_Imp`, `PowerP_Tot`) |
| `value` | Numeric reading |

### `measurelogic_summary`
One row per device/child per day with first/last/total for energy points and max for power/demand points.

## Environment Variables

| Variable | Description |
|---|---|
| `ML_USER` | MeasureLogic username |
| `ML_KEY` | MeasureLogic password |
| `DATABASE_URL` | Railway Postgres connection string |
| `TARGET_DATE` | (Optional) Override date as `YYYY-MM-DD`. Defaults to yesterday. |
| `DEVICE_LIMIT` | (Optional) Max devices to pull. Defaults to 18. |

## Railway Setup

1. Push this folder to a new GitHub repo.
2. In your existing Railway project → **+ New** → **GitHub Repo**.
3. Add the environment variables above in the Variables tab.
4. Set cron schedule: `0 9 * * *` (9 AM UTC / 3 AM Mountain).

## Local Test (PowerShell)

```powershell
$env:ML_USER="your_username"; $env:ML_KEY="your_password"; $env:DATABASE_URL="postgresql://..."; python measurelogic_ingest.py
```

## Backfill a Specific Date

Set `TARGET_DATE=2026-06-01` in Railway variables and trigger a manual deploy.
