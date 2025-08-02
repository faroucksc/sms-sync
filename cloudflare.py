#!/usr/bin/env python3
"""Cloudflare D1 API client for SMS Log Synchronization.

This module handles all interactions with the Cloudflare D1 API, including:
- Authentication
- Pagination
- Rate limiting
- Error handling
"""

import logging
import requests
import json
import time
from typing import Dict, List, Any, Optional, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import config

logger = logging.getLogger(__name__)


class CloudflareD1Client:
    """Client for interacting with Cloudflare D1 database."""

    def __init__(self):
        """Initialize the D1 client with configuration."""
        self.account_id = config.CLOUDFLARE_ACCOUNT_ID
        self.database_id = config.CLOUDFLARE_DATABASE_ID
        self.api_token = config.CLOUDFLARE_API_TOKEN
        self.table_name = config.CLOUDFLARE_D1_TABLENAME
        self.timeout = config.REQUEST_TIMEOUT
        self.max_retries = config.MAX_RETRIES

        # Base URL for API requests
        self.base_url = f"https://api.cloudflare.com/client/v4/accounts/{self.account_id}/d1/database/{self.database_id}"

        # Default headers for API requests
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

        # Setup session with retry logic
        self.session = self._setup_session()

    def _setup_session(self):
        """Create a requests session with retry configuration.

        Returns:
            requests.Session: Configured session object
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute an SQL query against the D1 database.

        Args:
            sql: SQL query string

        Returns:
            Dict: API response data

        Raises:
            Exception: If API call fails
        """
        url = f"{self.base_url}/query"
        data = {"sql": sql}

        try:
            response = self.session.post(
                url, headers=self.headers, json=data, timeout=self.timeout
            )
            response.raise_for_status()
            result = response.json()

            if not result.get("success"):
                error_msg = result.get("errors", [{"message": "Unknown API error"}])[
                    0
                ].get("message")
                logger.error(f"D1 API error: {error_msg}")
                raise Exception(f"D1 API error: {error_msg}")

            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"D1 API request failed: {str(e)}")
            raise

    def get_record_count(self) -> int:
        """Get the total number of records in the sms_logs table.

        Returns:
            int: Record count, or 0 on error
        """
        try:
            result = self.execute_query(
                f"SELECT COUNT(*) as count FROM {self.table_name}"
            )
            return result["result"][0]["results"][0]["count"]
        except Exception as e:
            logger.error(f"Failed to get record count: {str(e)}")
            return 0

    def get_records_batch(self, limit: int, offset: int) -> List[Dict[str, Any]]:
        """Get a batch of records from the specified offset.

        Args:
            limit: Maximum number of records to return
            offset: Starting record offset

        Returns:
            List: Records returned from the query

        Raises:
            Exception: If API call fails
        """
        sql = f"""
            SELECT id, source, msisdn, response, sent_date, sms_id, created_at 
            FROM {self.table_name} 
            LIMIT {limit} OFFSET {offset}
        """

        try:
            result = self.execute_query(sql)
            return result["result"][0]["results"]
        except Exception as e:
            logger.error(f"Failed to get records batch: {str(e)}")
            raise

    def test_connection(self) -> bool:
        """Test connection to D1 API.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.execute_query("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"D1 API connection test failed: {str(e)}")
            return False


if __name__ == "__main__":
    # Self-test code
    config.setup_logging()
    client = CloudflareD1Client()

    logger.info("Testing D1 API connection...")
    if client.test_connection():
        logger.info("D1 API connection successful")
        count = client.get_record_count()
        logger.info(f"Total records in {client.table_name}: {count}")

        if count > 0:
            logger.info("Fetching first batch...")
            records = client.get_records_batch(5, 0)
            logger.info(f"First record: {json.dumps(records[0], indent=2)}")
    else:
        logger.error("D1 API connection failed")
