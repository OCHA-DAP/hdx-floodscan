"""Publisher guard: are the blob intermediates fresh enough to publish from?

The upstream Databricks job "Run FloodScan" (ds-raster-pipelines) still
dispatches the GitHub publisher when it finishes (~23:20 UTC), BEFORE the
prepare job has run (00:15 UTC). Publishing then would re-publish yesterday's
numbers. So the workflow runs this first and only publishes when the manifest
was generated within --max-age-hours (default 6), or when forced.

Always exits 0 — a skip is a normal outcome, not a failure. Writes
``publish=true|false`` to $GITHUB_OUTPUT when set.
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import pg  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-age-hours", type=float, default=6)
    ap.add_argument("--force", default="false", help="'true' publishes whatever is in latest/")
    args = ap.parse_args()

    manifest = pg.read_manifest()
    if manifest is None:
        publish, reason = False, f"no intermediates under {pg.INTERMEDIATE_PREFIX}/latest/"
    else:
        generated_at = datetime.fromisoformat(manifest["generated_at"])
        age_h = (datetime.now(timezone.utc) - generated_at).total_seconds() / 3600
        detail = (
            f"manifest generated_at={manifest['generated_at']} ({age_h:.1f}h ago), "
            f"floodscan_max_date={manifest.get('floodscan_max_date')}, db_stage={manifest.get('db_stage')}"
        )
        if str(args.force).lower() == "true":
            publish, reason = True, f"forced; {detail}"
        elif age_h <= args.max_age_hours:
            publish, reason = True, f"fresh (<= {args.max_age_hours}h); {detail}"
        else:
            publish, reason = False, (
                f"stale (> {args.max_age_hours}h) — the prepare job has not run yet for this cycle; {detail}"
            )

    print(f"publish={'true' if publish else 'false'}: {reason}")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"publish={'true' if publish else 'false'}\n")


if __name__ == "__main__":
    main()
