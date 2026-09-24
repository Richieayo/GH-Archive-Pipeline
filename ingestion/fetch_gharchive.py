from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

LOG = logging.getLogger("gharchive")

GHARCHIVE_URL = "https://data.gharchive.org/{stamp}.json.gz"
DEFAULT_LAG_HOURS = 2
DEFAULT_VOLUME_PATH = "/Volumes/github_events/raw/landing"
CHUNK_SIZE = 1024 * 1024 



def hour_stamp(dt:datetime) -> str:
    """Format an hour the way GH Archive names its files: date padded, hour not."""
    return f"{dt:%Y-%m-%d}-{dt.hour}"

def target_hours(count: int, lag_hours: int, anchor:datetime | None = None) -> list[datetime]:
    """Return `count` hours ending at the most recent hour old enough to be published.
 
    Oldest first, so a partial failure still leaves a contiguous run of files.
    """

    if anchor is None:
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        newest = now - timedelta(hours=lag_hours)
    else:
        newest = anchor
    return [newest - timedelta(hours=offset) for offset in reversed(range(count))]


def build_client() -> WorkspaceClient:
    """Authenticate with the same DBT_-prefixed env vars the dbt profile uses."""
    host = os.environ.get("DBT_HOST")
    token = os.environ.get("DBT_TOKEN")
    if not host or not token:
        sys.exit("DBT_HOST and DBT_TOKEN must be set")
    if not host.startswith("https://"):
        host = f"https://{host}"
    return WorkspaceClient(host=host, token=token)


def already_landed(client: WorkspaceClient, remote_path: str) -> bool:
    try:
        client.files.get_metadata(remote_path)
        return True
    except NotFound:
        return False


def download(stamp: str, dest: Path) -> bool:
    """Stream one hourly file to local disk. Returns False if not yet published."""
    url = GHARCHIVE_URL.format(stamp=stamp)
    with requests.get(url, stream=True, timeout=120) as response:
        if response.status_code == 404:
            LOG.warning("%s not published yet (404) - skipping", stamp)
            return False
        response.raise_for_status()
        with dest.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                handle.write(chunk)
    LOG.info("downloaded %s (%.1f MB)", stamp, dest.stat().st_size / 1_048_576)
    return True


def land_hour(client:WorkspaceClient, dt:datetime, volume_path: str, overwrite: bool) -> str:
    """Download one hour and upload it to the volume. Returns a status string."""

    stamp = hour_stamp(dt)
    filename = f"{stamp}.json.gz"
    remote_path = f"{volume_path.rstrip('/')}/{filename}"


    if not overwrite and already_landed(client, remote_path):
        LOG.info("%s already in volume - skipping", filename)
        return "skipped"


    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / filename
        if not download(stamp, local_path):
            return "unavailable"
        with local_path.open("rb") as handle:
            client.files.upload(remote_path, handle, overwrite=overwrite)

    LOG.info("landed %s", remote_path)
    return "landed"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=1, help="how many consecutive hours to fetch")
    parser.add_argument(
        "--lag",
        type=int,
        default=DEFAULT_LAG_HOURS,
        help="how far behind now to start, to allow for publishing delay",
    )
    parser.add_argument(
        "--date",
        help="backfill anchor as YYYY-MM-DD-H or YYYY-MM-DD (defaults to hour 23); "
        "the run ends at this hour and works backwards",
    )
    parser.add_argument(
        "--volume-path",
        default=os.environ.get("LANDING_VOLUME_PATH", DEFAULT_VOLUME_PATH),
        help="Unity Catalog volume path to land files into",
    )
    parser.add_argument("--overwrite", action="store_true", help="re-land files already present")
    return parser.parse_args()
 
 
def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
 
    anchor = None
    if args.date:
        parts = args.date.split("-")
        if len(parts) == 4:
            anchor = datetime(*(int(p) for p in parts), tzinfo=timezone.utc)
        else:
            anchor = datetime(*(int(p) for p in parts), hour=23, tzinfo=timezone.utc)
 
    client = build_client()
    hours = target_hours(args.hours, args.lag, anchor)
    LOG.info("target range: %s -> %s", hour_stamp(hours[0]), hour_stamp(hours[-1]))
 
    tally: dict[str, int] = {"landed": 0, "skipped": 0, "unavailable": 0}
    for dt in hours:
        try:
            tally[land_hour(client, dt, args.volume_path, args.overwrite)] += 1
        except Exception:
            LOG.exception("failed on %s", hour_stamp(dt))
            tally["unavailable"] += 1
 
    LOG.info("done - landed=%(landed)d skipped=%(skipped)d unavailable=%(unavailable)d", tally)
    return 0 if tally["landed"] or tally["skipped"] else 1
 
 
if __name__ == "__main__":
    raise SystemExit(main())
 