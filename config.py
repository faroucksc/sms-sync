#!/usr/bin/env python3
"""Configuration management for SMS Log Synchronization System.

This module loads configuration from environment variables with sensible defaults.
It provides a central place to manage all system configuration.
"""
import os
from datetime import datetime
import logging


class Config:
    """Configuration container for the sync system."""

    def __init__(self):
        """Initialize configuration from environment variables."""
        # Cloudflare Configuration
        self.CLOUDFLARE_ACCOUNT_ID = os.getenv(
            "CLOUDFLARE_ACCOUNT_ID", "0d64926cc2139b2634bf04944498467a"
        )
        self.CLOUDFLARE_DATABASE_ID = os.getenv(
            "CLOUDFLARE_DATABASE_ID", "964dff8f-1216-465e-93b0-befb54ef8c32"
        )
        self.CLOUDFLARE_API_TOKEN = os.getenv(
            "CLOUDFLARE_API_TOKEN", "HGsrL8zvP8dNX9GWH1KAPVtfBtnuHnS1iu8NpTGU"
        )
        self.CLOUDFLARE_D1_TABLENAME = os.getenv("CLOUDFLARE_D1_TABLENAME", "sms_logs")

        # PostgreSQL Configuration
        self.POSTGRES_URL = os.getenv(
            "POSTGRES_URL",
            "postgres://postgres:Fasmopiosuaitdgf2323@fgdb.fgapps.ammalogic.com:2345/fgensms",
        )

        # Sync Configuration
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
        self.CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))
        self.REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

        # Default sync ID format
        self.SYNC_ID_FORMAT = os.getenv("SYNC_ID_FORMAT", "%Y%m%d%H%M%S")

        # Logging configuration
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_FILE = os.getenv("LOG_FILE", "d1_sync.log")
        self.LOG_FORMAT = os.getenv(
            "LOG_FORMAT", "%(asctime)s - %(levelname)s - %(message)s"
        )

        # Debug flag
        self.DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"

    def get_sync_id(self):
        """Generate a unique sync ID based on current timestamp."""
        return datetime.now().strftime(self.SYNC_ID_FORMAT)

    def setup_logging(self):
        """Configure logging based on configuration."""
        log_level = getattr(logging, self.LOG_LEVEL)

        # Configure file logging
        logging.basicConfig(
            filename=self.LOG_FILE,
            level=log_level,
            format=self.LOG_FORMAT,
        )

        # Add console handler
        console = logging.StreamHandler()
        console.setLevel(log_level)
        logging.getLogger("").addHandler(console)

        # Return logger for convenience
        return logging.getLogger("")


# Create a global instance for easy importing
config = Config()


if __name__ == "__main__":
    # Self-test code
    logger = config.setup_logging()
    logger.info("Configuration loaded successfully")
    logger.info(f"Cloudflare Account ID: {config.CLOUDFLARE_ACCOUNT_ID}")
    logger.info(f"PostgreSQL URL: {config.POSTGRES_URL}")
    logger.info(f"Batch Size: {config.BATCH_SIZE}")
    logger.info(f"Sync ID: {config.get_sync_id()}")
