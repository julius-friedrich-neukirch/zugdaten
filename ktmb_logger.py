#!/usr/bin/env python3
"""
KTMB live train position logger
===============================

Polls the Malaysian open-data GTFS Realtime feed for KTMB
(https://api.data.gov.my/gtfs-realtime/vehicle-position/ktmb)
and appends every train position to a daily CSV file.

Leave it running for a few weeks to build a dataset of actual train
movements, which can later be compared against the planned timetable
(GTFS Static) to estimate running times and delays.

Setup (once):
    pip install requests gtfs-realtime-bindings

Run:
    python ktmb_logger.py                  # poll every 30 s, write to ./ktmb_data
    python ktmb_logger.py --interval 60    # poll every 60 s
    python ktmb_logger.py --static         # also download today's GTFS Static timetable
    python ktmb_logger.py --once           # single poll, for testing

Stop with Ctrl+C. Restarting is safe: duplicates are skipped.

Output:
    ktmb_data/positions_YYYY-MM-DD.csv     one file per day (Malaysia time)
    ktmb_data/gtfs_static_YYYY-MM-DD.zip   timetable snapshot (with --static)
    ktmb_data/logger.log                   errors and status messages
"""

import argparse
import csv
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2

REALTIME_URL = "https://api.data.gov.my/gtfs-realtime/vehicle-position/ktmb"
STATIC_URL = "https://api.data.gov.my/gtfs-static/ktmb"
MYT = timezone(timedelta(hours=8))  # Malaysia time, UTC+8

FIELDS = [
    "fetched_at_utc",      # when this script received the feed
    "feed_timestamp_utc",  # when the feed was generated
    "position_time_utc",   # when the train reported this position
    "vehicle_id",
    "vehicle_label",
    "trip_id",
    "route_id",
    "direction_id",
    "start_date",
    "start_time",
    "latitude",
    "longitude",
    "bearing",
    "speed_mps",           # metres per second, as reported by the feed
    "stop_id",
    "current_stop_sequence",
    "current_status",      # INCOMING_AT / STOPPED_AT / IN_TRANSIT_TO
]

STATUS_NAMES = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}

_running = True


def _stop(signum, frame):
    global _running
    _running = False
    logging.info("Stop requested, finishing current poll...")


def utc_iso(ts):
    """Unix seconds -> ISO-8601 UTC string, or empty if missing."""
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def feed_to_rows(raw_bytes, fetched_at):
    """Parse a GTFS-RT protobuf payload into a header timestamp and a list of row dicts."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)
    feed_ts = feed.header.timestamp if feed.header.HasField("timestamp") else 0

    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        trip = v.trip
        pos = v.position
        vid = v.vehicle.id or v.vehicle.label or entity.id
        rows.append({
            "fetched_at_utc": fetched_at.isoformat(),
            "feed_timestamp_utc": utc_iso(feed_ts),
            "position_time_utc": utc_iso(v.timestamp) if v.HasField("timestamp") else "",
            "vehicle_id": vid,
            "vehicle_label": v.vehicle.label,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
            "direction_id": trip.direction_id if trip.HasField("direction_id") else "",
            "start_date": trip.start_date,
            "start_time": trip.start_time,
            "latitude": round(pos.latitude, 6) if v.HasField("position") else "",
            "longitude": round(pos.longitude, 6) if v.HasField("position") else "",
            "bearing": pos.bearing if pos.HasField("bearing") else "",
            "speed_mps": round(pos.speed, 2) if pos.HasField("speed") else "",
            "stop_id": v.stop_id,
            "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else "",
            "current_status": STATUS_NAMES.get(v.current_status, "") if v.HasField("current_status") else "",
        })
    return feed_ts, rows


def row_key(row):
    """Identifies a unique report: same train, same reported time, same place."""
    return (row["vehicle_id"], row["position_time_utc"] or row["feed_timestamp_utc"],
            str(row["latitude"]), str(row["longitude"]))


class DailyCsv:
    """Appends rows to positions_YYYY-MM-DD.csv, switching files at Malaysian midnight."""

    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.day = None
        self.path = None
        self.seen = set()

    def _open_day(self, day):
        self.day = day
        self.path = self.out_dir / f"positions_{day}.csv"
        self.seen = set()
        if self.path.exists():  # resume after restart: remember what is already saved
            with self.path.open(newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    self.seen.add(row_key(r))
        else:
            with self.path.open("w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=FIELDS).writeheader()

    def write(self, rows, now_utc):
        day = now_utc.astimezone(MYT).strftime("%Y-%m-%d")
        if day != self.day:
            self._open_day(day)
        new = [r for r in rows if row_key(r) not in self.seen]
        if new:
            with self.path.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=FIELDS)
                w.writerows(new)
            self.seen.update(row_key(r) for r in new)
        return len(new)


def download_static(session, out_dir, url):
    day = datetime.now(MYT).strftime("%Y-%m-%d")
    path = out_dir / f"gtfs_static_{day}.zip"
    if path.exists():
        logging.info("Static timetable for %s already saved: %s", day, path.name)
        return
    r = session.get(url, timeout=60)
    r.raise_for_status()
    path.write_bytes(r.content)
    logging.info("Saved static timetable: %s (%d KB)", path.name, len(r.content) // 1024)


def main():
    p = argparse.ArgumentParser(description="Log KTMB live train positions to CSV.")
    p.add_argument("--interval", type=int, default=30, help="seconds between polls (default 30)")
    p.add_argument("--out", default="ktmb_data", help="output folder (default ./ktmb_data)")
    p.add_argument("--static", action="store_true", help="also download today's GTFS Static timetable")
    p.add_argument("--once", action="store_true", help="poll once and exit (for testing)")
    p.add_argument("--url", default=REALTIME_URL, help=argparse.SUPPRESS)
    p.add_argument("--static-url", default=STATIC_URL, help=argparse.SUPPRESS)
    args = p.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(out_dir / "logger.log", encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)],
    )
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    session = requests.Session()
    session.headers["User-Agent"] = "ktmb-research-logger/1.0"

    if args.static:
        try:
            download_static(session, out_dir, args.static_url)
        except Exception as e:
            logging.error("Static download failed: %s", e)

    writer = DailyCsv(out_dir)
    last_feed_ts = None
    failures = 0
    logging.info("Logging KTMB positions every %ds into %s (Ctrl+C to stop)", args.interval, out_dir.resolve())

    while _running:
        started = time.monotonic()
        try:
            r = session.get(args.url, timeout=20)
            r.raise_for_status()
            now = datetime.now(timezone.utc)
            feed_ts, rows = feed_to_rows(r.content, now)
            if feed_ts and feed_ts == last_feed_ts:
                logging.info("Feed unchanged since last poll, skipped")
            else:
                added = writer.write(rows, now)
                logging.info("%d trains in feed, %d new rows -> %s", len(rows), added, writer.path.name)
                last_feed_ts = feed_ts
            failures = 0
        except Exception as e:
            failures += 1
            logging.error("Poll failed (%d in a row): %s", failures, e)

        if args.once:
            break
        # back off after repeated failures (max 10 min), otherwise keep the normal rhythm
        wait = args.interval if failures == 0 else min(args.interval * 2 ** min(failures, 5), 600)
        wait -= time.monotonic() - started
        end = time.monotonic() + max(wait, 0)
        while _running and time.monotonic() < end:
            time.sleep(min(1, end - time.monotonic()))

    logging.info("Stopped.")


if __name__ == "__main__":
    main()
