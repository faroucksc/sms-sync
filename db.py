#!/usr/bin/env python3
"""PostgreSQL database operations for SMS Log Synchronization.

This module handles all PostgreSQL database interactions, including:
- Connection management
- Transaction handling
- Query execution
- Error handling
"""

import logging
import psycopg2
import psycopg2.extras
from contextlib import closing
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional, Union

from .config import config

logger = logging.getLogger(__name__)


def get_db_connection():
    """Create a new PostgreSQL database connection.

    Returns:
        psycopg2.connection: A PostgreSQL connection object
    """
    conn = psycopg2.connect(
        config.POSTGRES_URL, options="-c statement_timeout=300000"  # 5 minute timeout
    )
    conn.set_session(autocommit=False)
    return conn


def verify_schema():
    """Verify the PostgreSQL schema exists and create indices if needed.

    Returns:
        bool: True if schema is valid, False otherwise
    """
    try:
        with closing(get_db_connection()) as conn:
            with closing(conn.cursor()) as cursor:
                # Check if table exists
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'fgensms'
                    );
                """
                )
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    logger.error("Table 'fgensms' does not exist in PostgreSQL")
                    return False

                # Create indices if they don't exist
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_fgensms_sync_id 
                    ON fgensms(sync_execution_id);
                """
                )

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_fgensms_created_at 
                    ON fgensms(created_at);
                """
                )

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_fgensms_sent_date 
                    ON fgensms(sent_date);
                """
                )

                # Commit index creation
                conn.commit()

                logger.info("PostgreSQL schema validated and indices created")
                return True

    except Exception as e:
        logger.error(f"Error verifying PostgreSQL schema: {str(e)}")
        return False


def get_record_count():
    """Get the total record count in the fgensms table.

    Returns:
        int: The number of records, or 0 on error
    """
    try:
        with closing(get_db_connection()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute("SELECT COUNT(*) FROM fgensms")
                return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting record count: {str(e)}")
        return 0


def process_batch(cursor, records: List[Dict], sync_id: str):
    """Process a batch of records using execute_values for efficiency.

    Args:
        cursor: A PostgreSQL cursor
        records: List of record dictionaries
        sync_id: Current sync execution ID
    """
    values = [
        (
            record.get("id"),
            record.get("source"),
            record.get("msisdn"),
            record.get("response"),
            record.get("sent_date"),  # Will be normalized before this function
            record.get("sms_id"),
            record.get("created_at"),  # Will be normalized before this function
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
        page_size=config.CHUNK_SIZE,
    )


def test_connection():
    """Test the PostgreSQL connection.

    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        with closing(get_db_connection()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone()[0] == 1
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {str(e)}")
        return False


if __name__ == "__main__":
    # Self-test code
    config.setup_logging()
    logger.info("Testing PostgreSQL connection...")
    if test_connection():
        logger.info("PostgreSQL connection successful")
    else:
        logger.error("PostgreSQL connection failed")

    logger.info("Verifying schema...")
    if verify_schema():
        logger.info("Schema verification successful")
        count = get_record_count()
        logger.info(f"Current record count: {count}")
    else:
        logger.error("Schema verification failed")
