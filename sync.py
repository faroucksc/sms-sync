#!/usr/bin/env python3
import requests
import psycopg2
import psycopg2.extras  # For executemany
import re
import logging
import time
import json
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from contextlib import closing
from psycopg2.extras import execute_values

# Set up logging
logging.basicConfig(
    filename="d1_sync.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

# Configuration
CLOUDFLARE_ACCOUNT_ID = "0d64926cc2139b2634bf04944498467a"
CLOUDFLARE_DATABASE_ID = "964dff8f-1216-465e-93b0-befb54ef8c32"
CLOUDFLARE_API_TOKEN = "HGsrL8zvP8dNX9GWH1KAPVtfBtnuHnS1iu8NpTGU"
CLOUDFLARE_D1_TABLENAME = "sms_logs"
POSTGRES_URL = (
    "postgres://postgres:Fasmopiosuaitdgf2323@fgdb.fgapps.ammalogic.com:2345/fgensms"
)
BATCH_SIZE = 5000  # Increased batch size
CHUNK_SIZE = 1000  # Size for execute_values
DEBUG_MODE = True  # Add debug mode flag
COMMIT_EVERY = 100  # Commit more frequently

# Request configuration
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3


# Setup requests session with retries
def setup_requests_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


# Function to normalize dates
def normalize_date(date_str):
    if not date_str:
        return None

    # Already in ISO format
    if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", date_str):
        return date_str

    # MM/DD/YYYY h:mm:ss AM/PM format
    mdy_match = re.match(
        r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})\s+(AM|PM)$", date_str
    )
    if mdy_match:
        month, day, year, hour, minute, second, am_pm = mdy_match.groups()
        hour = int(hour)
        if am_pm == "PM" and hour < 12:
            hour += 12
        elif am_pm == "AM" and hour == 12:
            hour = 0
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}T{str(hour).zfill(2)}:{minute}:{second}Z"

    # YYYY-MM-DD h:mm:ss AM/PM format
    ymd_match = re.match(
        r"^(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})\s+(AM|PM)$", date_str
    )
    if ymd_match:
        year, month, day, hour, minute, second, am_pm = ymd_match.groups()
        hour = int(hour)
        if am_pm == "PM" and hour < 12:
            hour += 12
        elif am_pm == "AM" and hour == 12:
            hour = 0
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}T{str(hour).zfill(2)}:{minute}:{second}Z"

    # Basic conversion
    try:
        if " " in date_str and "T" not in date_str:
            return date_str.replace(" ", "T") + ("" if date_str.endswith("Z") else "Z")
        return date_str + ("" if date_str.endswith("Z") else "Z")
    except:
        return date_str


def get_db_connection():
    conn = psycopg2.connect(
        POSTGRES_URL, options="-c statement_timeout=300000"  # 5 minute timeout
    )
    conn.set_session(autocommit=False)
    return conn


def process_batch(cursor, records, sync_id):
    values = [
        (
            record.get("id"),
            record.get("source"),
            record.get("msisdn"),
            record.get("response"),
            normalize_date(record.get("sent_date")),
            record.get("sms_id"),
            normalize_date(record.get("created_at")),
            sync_id,
        )
        for record in records
    ]

    execute_values(
        cursor,
        """
        INSERT INTO fgensms 
        (id, source, msisdn, response, sent_date, sms_id, created_at, sync_execution_id)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            source = EXCLUDED.source,
            msisdn = EXCLUDED.msisdn,
            response = EXCLUDED.response,
            sent_date = EXCLUDED.sent_date,
            sms_id = EXCLUDED.sms_id,
            created_at = EXCLUDED.created_at,
            sync_execution_id = EXCLUDED.sync_execution_id
        """,
        values,
        page_size=CHUNK_SIZE,
    )


def main():
    start_time = time.time()
    sync_id = datetime.now().strftime("%Y%m%d%H%M%S")
    logging.info(f"Starting sync with ID: {sync_id}")
    session = setup_requests_session()

    try:
        # Initial connection for counts
        with closing(get_db_connection()) as conn:
            with closing(conn.cursor()) as cursor:
                # Connect to PostgreSQL
                logging.info("Connecting to PostgreSQL...")

                # Test connection with simple query
                cursor.execute("SELECT 1")
                logging.info("PostgreSQL connection successful")

                # Get count from D1
                logging.info("Fetching count from D1...")
                d1_headers = {
                    "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
                    "Content-Type": "application/json",
                }
                d1_url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{CLOUDFLARE_DATABASE_ID}/query"
                d1_data = {"sql": "SELECT COUNT(*) as count FROM sms_logs"}

                response = session.post(
                    d1_url, headers=d1_headers, json=d1_data, timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                d1_result = response.json()
                d1_count = d1_result["result"][0]["results"][0]["count"]

                # Get count from PostgreSQL
                cursor.execute("SELECT COUNT(*) FROM fgensms")
                pg_count = cursor.fetchone()[0]

                # Calculate delta
                delta = max(0, d1_count - pg_count)
                total_batches = (delta + BATCH_SIZE - 1) // BATCH_SIZE

                logging.info(
                    f"D1 Count: {d1_count}, PostgreSQL Count: {pg_count}, Delta: {delta}"
                )

                if delta == 0:
                    logging.info("No new records to sync")
                    return

        # Process batches with fresh connections
        for batch_num in range(total_batches):
            with closing(get_db_connection()) as conn:
                with closing(conn.cursor()) as cursor:
                    batch_start = time.time()
                    offset = pg_count + (batch_num * BATCH_SIZE)

                    # Fetch batch from D1
                    d1_data = {
                        "sql": f"SELECT id, source, msisdn, response, sent_date, sms_id, created_at FROM sms_logs LIMIT {BATCH_SIZE} OFFSET {offset}"
                    }

                    response = session.post(
                        d1_url,
                        headers=d1_headers,
                        json=d1_data,
                        timeout=REQUEST_TIMEOUT,
                    )
                    response.raise_for_status()
                    result = response.json()

                    if not result.get("success"):
                        raise Exception("D1 API request failed")

                    records = result["result"][0]["results"]
                    if not records:
                        break

                    # Process batch
                    process_start = time.time()
                    process_batch(cursor, records, sync_id)
                    conn.commit()
                    process_end = time.time()

                    # Simple progress logging
                    logging.info(
                        f"Batch {batch_num+1}/{total_batches}: "
                        f"Processed {len(records)} records in "
                        f"{process_end - process_start:.2f}s "
                        f"({len(records)/(process_end - process_start):.1f} records/s)"
                    )

                    if len(records) < BATCH_SIZE:
                        break

    except requests.exceptions.Timeout:
        logging.error("Request to D1 API timed out")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"Request to D1 API failed: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error during sync: {str(e)}")
        raise


if __name__ == "__main__":
    main()
