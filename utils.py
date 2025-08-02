#!/usr/bin/env python3
"""Utility functions for SMS Log Synchronization.

This module contains various utility functions used across the system.
"""

import logging
import re
from datetime import datetime
import time
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """Normalize various date formats to ISO format.

    Args:
        date_str: Date string in various formats

    Returns:
        Normalized date string in ISO format, or None if input is None
    """
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


def timed(func: Callable) -> Callable:
    """Decorator to measure and log execution time of functions.

    Args:
        func: Function to time

    Returns:
        Wrapped function that logs execution time
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(
            f"Function {func.__name__} took {end_time - start_time:.2f} seconds"
        )
        return result

    return wrapper


def format_bytes(size: int) -> str:
    """Format byte size to human readable string.

    Args:
        size: Size in bytes

    Returns:
        Human readable string with size in KB, MB, etc.
    """
    if size < 1024:
        return f"{size} B"

    for unit in ["KB", "MB", "GB", "TB"]:
        size /= 1024
        if size < 1024:
            return f"{size:.2f} {unit}"

    return f"{size:.2f} PB"


def chunked_list(items: list, chunk_size: int):
    """Split a list into chunks of specified size.

    Args:
        items: List to split
        chunk_size: Size of each chunk

    Yields:
        Lists of specified chunk size
    """
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def retry(
    max_attempts: int, delay: float = 1.0, backoff: float = 2.0, exceptions=(Exception,)
):
    """Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier
        exceptions: Exceptions to catch

    Returns:
        Decorated function with retry logic
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            mtries, mdelay = max_attempts, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(
                        f"Function {func.__name__} failed: {str(e)}. Retrying in {mdelay} seconds..."
                    )
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return func(*args, **kwargs)

        return wrapper

    return decorator


if __name__ == "__main__":
    # Self-test code
    logging.basicConfig(level=logging.INFO)

    # Test date normalization
    test_dates = [
        "2023-01-01T12:30:45Z",  # Already ISO
        "1/15/2023 2:30:45 PM",  # MM/DD/YYYY
        "2023-1-15 2:30:45 PM",  # YYYY-MM-DD
        "2023-01-15 14:30:45",  # Basic date time
        None,  # None value
    ]

    for date in test_dates:
        normalized = normalize_date(date)
        logger.info(f"Original: {date}, Normalized: {normalized}")

    # Test timed decorator
    @timed
    def slow_function():
        time.sleep(1)
        return "Done"

    logger.info(slow_function())

    # Test chunked list
    items = list(range(10))
    for chunk in chunked_list(items, 3):
        logger.info(f"Chunk: {chunk}")
