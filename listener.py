#!/usr/bin/env python3
"""
garage_monitor/listener.py

Listens to rtl_433 output, parses garage door RF events,
and inserts them into a PostgreSQL database.

Usage:
    source venv/bin/activate
    python listener.py
"""

import json
import os
import subprocess
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import Json


# ── Load .env config ──────────────────────────────────────────────────────────
# Reads DB_HOST, DB_USER, DB_PASS, etc. from your .env file
load_dotenv()


# ── Logging setup ─────────────────────────────────────────────────────────────
# Writes timestamped messages to both the terminal and listener.log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),              # prints to terminal
        logging.FileHandler("listener.log")   # saves to file
    ]
)
log = logging.getLogger(__name__)


# ── Database connection ───────────────────────────────────────────────────────
def get_connection():
    """
    Opens a connection to PostgreSQL using credentials from .env.
    Returns the connection object.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )


# ── Insert a single event into the database ───────────────────────────────────
def insert_event(cur, event: dict):
    """
    Takes a parsed rtl_433 event (as a Python dict) and inserts it
    into the raw_events table.

    cur   = the database cursor (our "pen" for writing SQL)
    event = the dictionary parsed from rtl_433's JSON output
    """

    # rtl_433 gives us a time string like "2024-01-15 09:23:11"
    # We convert it to a proper datetime object so Postgres stores it correctly
    device_time = None
    if "time" in event:
        try:
            device_time = datetime.fromisoformat(event["time"])
            # If no timezone info, assume UTC
            if device_time.tzinfo is None:
                device_time = device_time.replace(tzinfo=timezone.utc)
        except ValueError:
            # If the time format is unexpected, just leave it as None
            pass

    # %s are safe placeholders — psycopg2 fills these in securely
    # Never build SQL with f-strings or string concatenation (SQL injection risk)
    cur.execute(
        """
        INSERT INTO raw_events
            (device_time, model, count, num_rows, rows, codes, raw_payload)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            device_time,
            event.get("model"),           # the name set in your -X decoder ("name")
            event.get("count"),           # how many times the signal repeated
            event.get("num_rows"),        # number of data rows captured
            Json(event.get("rows")),      # e.g. [{"len": 10, "data": "aa8"}]
            Json(event.get("codes")),     # e.g. ["{10}aa8"]
            Json(event),                  # full raw JSON stored as backup
        )
    )


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    """
    Launches rtl_433 as a subprocess, reads its JSON output line by line,
    and saves each event to the database.
    """

    # Pull config from .env
    freq = os.getenv("RTL_FREQ", "300M")
    model_filter = os.getenv("DEVICE_MODEL", "")

    # The command we're running — equivalent to typing this in the terminal:
    # rtl_433 -f 300M -s 1024k -X "n=name,m=OOK_PWM,..." -F json -M utc
    # -f 300M        = tune to 300 MHz
    # -s 1024k       = sample rate of 1024k samples/sec
    # -X "..."       = custom OOK_PWM decoder with your door's pulse timings
    # -F json        = output decoded events as JSON
    # -M utc         = timestamp events in UTC
    cmd = [
        "rtl_433",
        "-f", freq,
        "-s", "1024k",
        "-X", "n=name,m=OOK_PWM,s=512,l=1538,r=16000,bits=10,tolerance=200",
        "-F", "json",
        "-M", "utc",
    ]
    log.info(f"Starting rtl_433: {' '.join(cmd)}")
    if model_filter:
        log.info(f"Filtering to device model: '{model_filter}'")
    else:
        log.info("No model filter set — capturing all RF events")

    # Connect to the database
    try:
        conn = get_connection()
        conn.autocommit = True   # each INSERT commits immediately, no manual commits needed
        cur = conn.cursor()
        log.info("Connected to PostgreSQL successfully.")
    except Exception as e:
        log.error(f"Could not connect to database: {e}")
        log.error("Check your .env file and make sure PostgreSQL is running.")
        return

    # Launch rtl_433 and pipe its output to us
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,    # capture stdout so Python can read it
        stderr=subprocess.DEVNULL, # silence rtl_433's noisy startup messages
        text=True,                 # give us strings, not raw bytes
    )
    log.info("rtl_433 is running. Press your garage remote to test...")

    try:
        # This loop blocks and waits — it runs forever until you stop the script
        # Each iteration processes one line of JSON from rtl_433
        for line in proc.stdout:
            line = line.strip()

            # Skip blank lines
            if not line:
                continue

            # Parse the JSON text into a Python dictionary
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                # rtl_433 sometimes prints non-JSON lines at startup — just skip them
                log.debug(f"Skipping non-JSON line: {line}")
                continue

            # If a model filter is set, ignore events from other devices
            if model_filter and event.get("model") != model_filter:
                log.debug(f"Ignoring event from model: {event.get('model')}")
                continue

            # Try to save the event to the database
            try:
                insert_event(cur, event)
                log.info(
                    f"Event saved — model={event.get('model')} | "
                    f"count={event.get('count')} | "
                    f"num_rows={event.get('num_rows')} | "
                    f"codes={event.get('codes')}"
                )
            except Exception as e:
                log.error(f"Failed to insert event: {e}")
                log.error(f"Event data was: {event}")
                # Roll back any partial transaction before continuing
                conn.rollback()

    except KeyboardInterrupt:
        # User pressed Ctrl+C — shut down cleanly
        log.info("Ctrl+C received. Shutting down gracefully...")

    except Exception as e:
        log.error(f"Unexpected error in main loop: {e}")

    finally:
        # This block ALWAYS runs — even on crash or Ctrl+C
        # Always clean up your resources
        log.info("Cleaning up...")
        proc.terminate()   # stop rtl_433
        cur.close()        # close the database cursor
        conn.close()       # close the database connection
        log.info("Shutdown complete.")


# ── Entry point ───────────────────────────────────────────────────────────────
# This means: only run main() if this file is executed directly
# (not if it's imported by another script)
if __name__ == "__main__":
    main()
