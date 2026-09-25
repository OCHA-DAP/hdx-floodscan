"""Databricks half of the HDX FloodScan publish: run the DB queries, park the
results on blob, then wake the GitHub publisher.

The rasterstats DB is reachable only through its private endpoint, so the
three ``public.floodscan`` queries the publish needs (yearly maxima for the
return periods, the 10-year day-of-year baseline, the last 90 days) run here
and are written as parquet to the dev blob::

    projects/hdx-floodscan/intermediate/<YYYY-MM-DD>/  (dated copy, pruned after KEEP_DAYS)
    projects/hdx-floodscan/intermediate/latest/        (what the publisher reads)

plus ``manifest.json`` (generated_at, db_stage, floodscan_max_date, row counts).
Then the GitHub workflow ``run-python-script.yaml`` is dispatched with the
``GH_FLOODSCAN_TOKEN`` (dsci secret, exposed as an env var by the wrapper).
The HDX credentials never leave GitHub.

    python scripts/prepare_intermediates.py                # STAGE (default prod) = DB to read
    python scripts/prepare_intermediates.py --no-dispatch  # local: park intermediates only
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

import ocha_stratus as stratus  # noqa: E402

from src.utils import pg  # noqa: E402
from trigger_webhook import trigger_workflow  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("azure").setLevel(logging.WARNING)  # no per-request SDK chatter
logger = logging.getLogger(__name__)

# What floodscan.py asks pg for: admin 1 + 2, SFED, HRP countries only for the
# current/baseline series, all countries for the yearly maxima.
ADMIN_LEVELS = (1, 2)
BAND = "SFED"
KEEP_DAYS = 14
GH_ACCOUNT, GH_REPO, GH_WORKFLOW = "OCHA-DAP", "hdx-floodscan", "run-python-script.yaml"


def _upload(df, kind, admin_level, only_HRP, folders):
    for folder in folders:
        stratus.upload_parquet_to_blob(
            df,
            pg.intermediate_blob_name(kind, admin_level, BAND, only_HRP, folder=folder),
            stage=pg.INTERMEDIATE_STAGE,
            container_name=pg.INTERMEDIATE_CONTAINER,
        )


def prepare(db_stage):
    engine = pg.get_engine(db_stage)
    generated_at = datetime.now(timezone.utc)
    today = generated_at.strftime("%Y-%m-%d")
    folders = ("latest", today)
    rows = {}
    max_dates = []
    for adm in ADMIN_LEVELS:
        yr_max = pg.query_year_max(engine, adm, BAND)
        rolling = pg.query_rolling_11_day_mean(engine, adm, BAND, only_HRP=True)
        last90 = pg.query_last_90_days(engine, adm, BAND, only_HRP=True)
        if last90.empty:
            raise RuntimeError(f"last_90_days is empty for adm{adm} — refusing to publish nothing")
        _upload(yr_max, "year_max", adm, False, folders)
        _upload(rolling, "rolling_11_day_mean", adm, True, folders)
        _upload(last90, "last_90_days", adm, True, folders)
        rows[f"adm{adm}"] = {
            "year_max": len(yr_max),
            "rolling_11_day_mean": len(rolling),
            "last_90_days": len(last90),
        }
        max_dates.append(str(last90["valid_date"].max()))
        logger.info("adm%s: %s", adm, rows[f"adm{adm}"])

    manifest = {
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "db_stage": db_stage,
        "band": BAND,
        "admin_levels": list(ADMIN_LEVELS),
        "floodscan_max_date": max(max_dates),
        "rows": rows,
    }
    for folder in folders:
        stratus.upload_blob_data(
            json.dumps(manifest, indent=1),
            pg.manifest_blob_name(folder),
            stage=pg.INTERMEDIATE_STAGE,
            container_name=pg.INTERMEDIATE_CONTAINER,
            content_type="application/json",
        )
    logger.info("manifest: %s", manifest)
    return manifest


def prune(keep_days=KEEP_DAYS):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    client = stratus.get_container_client(
        pg.INTERMEDIATE_CONTAINER, stage=pg.INTERMEDIATE_STAGE, write=True
    )
    removed = 0
    for blob in client.list_blobs(name_starts_with=pg.INTERMEDIATE_PREFIX + "/"):
        folder = blob.name[len(pg.INTERMEDIATE_PREFIX) + 1 :].split("/", 1)[0]
        if folder != "latest" and len(folder) == 10 and folder < cutoff and "." in blob.name.rsplit("/", 1)[-1]:
            client.delete_blob(blob.name)
            removed += 1
    logger.info("pruned %s blobs older than %s", removed, cutoff)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-dispatch", action="store_true", help="don't trigger the GitHub publisher")
    ap.add_argument("--no-prune", action="store_true")
    args, _unknown = ap.parse_known_args()

    db_stage = os.environ.get("STAGE", "prod")
    prepare(db_stage)
    if not args.no_prune:
        prune()
    if args.no_dispatch:
        logger.info("--no-dispatch: not triggering %s/%s %s", GH_ACCOUNT, GH_REPO, GH_WORKFLOW)
        return
    trigger_workflow(GH_ACCOUNT, GH_REPO, GH_WORKFLOW)


if __name__ == "__main__":
    main()
