"""
MeasureLogic Daily Ingest Pipeline
====================================
Pulls yesterday's interval data from the FieldPop/MeasureLogic API
and upserts into PostgreSQL:
  - measurelogic_interval : one row per timestamp/device/child with a column per point

Environment variables required:
  ML_USER       — MeasureLogic username
  ML_KEY        — MeasureLogic password
  DATABASE_URL  — PostgreSQL connection string (Railway Postgres)

Optional:
  TARGET_DATE   — Override date to pull (YYYY-MM-DD). Defaults to yesterday.
  DEVICE_LIMIT  — Max number of devices to pull (default: 18)
"""

import os
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ML_USER      = os.environ["ML_USER"]
ML_KEY       = os.environ["ML_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
DEVICE_LIMIT = int(os.environ.get("DEVICE_LIMIT", 18))

BASE_URL        = "https://www.fieldpop.io/rest"
LOGIN_URL       = f"{BASE_URL}/login?username={ML_USER}&password={ML_KEY}"
DEVICE_LIST_URL = f"{BASE_URL}/method/fieldpop-api/getUserDevices?list=true&happn_token={{token}}"
DEVICE_DATA_URL = (
    f"{BASE_URL}/method/fieldpop-api/deviceDataLog"
    f"?deviceID={{device_id}}&happn_token={{token}}&startUTCsec={{start}}&endUTCsec={{end}}"
)

SKIP_DEVICES = {"hailwalker_f3D43LzdF"}

INCLUDED_POINTS = [
    "EnergyP_Tot_Imp",
    "EnergyP_Tot_Exp",
    "EnergyP_Inv_Imp",
    "PowerP_Tot",
    "PowerP_Inv",
    "DemandP_Tot",
]

# ── Date resolution ───────────────────────────────────────────────────────────
def resolve_target_date():
    raw = os.environ.get("TARGET_DATE", "").strip()
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()


def date_to_epoch_range(d):
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end   = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


# ── API helpers ───────────────────────────────────────────────────────────────
def login():
    r = requests.get(LOGIN_URL, timeout=30)
    r.raise_for_status()
    return r.json()["data"]["token"]


def get_devices(token):
    url = DEVICE_LIST_URL.format(token=token)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


def get_device_data(token, device_id, start_utc, end_utc):
    url = DEVICE_DATA_URL.format(
        device_id=device_id, token=token, start=start_utc, end=end_utc
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("data", {})


# ── DB ────────────────────────────────────────────────────────────────────────
CREATE_INTERVAL_TABLE = """
CREATE TABLE IF NOT EXISTS measurelogic_interval (
    id                BIGSERIAL PRIMARY KEY,
    date              DATE             NOT NULL,
    timestamp_utc     TIMESTAMP        NOT NULL,
    device_id         TEXT             NOT NULL,
    child_id          TEXT             NOT NULL,
    energyp_tot_imp   DOUBLE PRECISION,
    energyp_tot_exp   DOUBLE PRECISION,
    energyp_inv_imp   DOUBLE PRECISION,
    powerp_tot        DOUBLE PRECISION,
    powerp_inv        DOUBLE PRECISION,
    demandp_tot       DOUBLE PRECISION,
    inserted_at       TIMESTAMP        NOT NULL DEFAULT NOW(),
    CONSTRAINT measurelogic_interval_uq UNIQUE (timestamp_utc, device_id, child_id)
);
"""

UPSERT_INTERVAL = """
INSERT INTO measurelogic_interval
    (date, timestamp_utc, device_id, child_id,
     energyp_tot_imp, energyp_tot_exp, energyp_inv_imp,
     powerp_tot, powerp_inv, demandp_tot)
VALUES %s
ON CONFLICT (timestamp_utc, device_id, child_id)
DO UPDATE SET
    energyp_tot_imp = COALESCE(EXCLUDED.energyp_tot_imp, measurelogic_interval.energyp_tot_imp),
    energyp_tot_exp = COALESCE(EXCLUDED.energyp_tot_exp, measurelogic_interval.energyp_tot_exp),
    energyp_inv_imp = COALESCE(EXCLUDED.energyp_inv_imp, measurelogic_interval.energyp_inv_imp),
    powerp_tot      = COALESCE(EXCLUDED.powerp_tot,      measurelogic_interval.powerp_tot),
    powerp_inv      = COALESCE(EXCLUDED.powerp_inv,      measurelogic_interval.powerp_inv),
    demandp_tot     = COALESCE(EXCLUDED.demandp_tot,     measurelogic_interval.demandp_tot),
    inserted_at     = NOW();
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    target_date = resolve_target_date()
    log.info(f"=== MeasureLogic ingest for {target_date} ===")

    start_epoch, end_epoch = date_to_epoch_range(target_date)

    token = login()
    log.info("Logged in to MeasureLogic API.")

    devices = get_devices(token)
    log.info(f"Retrieved {len(devices)} devices; pulling up to {DEVICE_LIMIT}.")

    # keyed by (device_id, child_id, timestamp_utc) → {point_name: value}
    rows = defaultdict(lambda: {p: None for p in INCLUDED_POINTS})

    for device_id in devices[:DEVICE_LIMIT]:
        if device_id in SKIP_DEVICES:
            log.info(f"  Skipping {device_id}")
            continue

        log.info(f"  Pulling {device_id} ...")
        try:
            device_data = get_device_data(token, device_id, start_epoch, end_epoch)
        except Exception as e:
            log.error(f"  Error fetching {device_id}: {e}")
            continue

        if not isinstance(device_data, dict):
            log.warning(f"  Unexpected data format for {device_id}")
            continue

        for child_id, points_dict in device_data.items():
            for point_name, datapoints in points_dict.items():
                if point_name not in INCLUDED_POINTS:
                    continue
                for entry in datapoints:
                    ts  = datetime.utcfromtimestamp(entry["time"])
                    key = (device_id, child_id, ts)
                    rows[key][point_name] = entry["value"]

    if not rows:
        log.warning("No data collected. Exiting.")
        return

    interval_rows = [
        (
            target_date, ts, device_id, child_id,
            points["EnergyP_Tot_Imp"],
            points["EnergyP_Tot_Exp"],
            points["EnergyP_Inv_Imp"],
            points["PowerP_Tot"],
            points["PowerP_Inv"],
            points["DemandP_Tot"],
        )
        for (device_id, child_id, ts), points in rows.items()
    ]

    log.info(f"Collected {len(interval_rows)} interval rows.")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_INTERVAL_TABLE)
        conn.commit()

        with conn.cursor() as cur:
            execute_values(cur, UPSERT_INTERVAL, interval_rows)
        conn.commit()
        log.info(f"Upserted {len(interval_rows)} rows into measurelogic_interval.")

    finally:
        conn.close()

    log.info(f"=== Done. MeasureLogic ingest complete for {target_date} ===")


if __name__ == "__main__":
    main()
