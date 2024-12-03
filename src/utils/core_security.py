import logging
import os
from datetime import datetime
from functools import wraps
from pathlib import Path

import yaml


def secure_config(env: str = "dev") -> dict:
    """Load configuration with sensitive values from environment."""
    config_path = Path("config") / f"{env}.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Override sensitive values with environment variables
    if os.getenv("ETL_API_KEY"):
        config["security"]["api_key"] = os.getenv("ETL_API_KEY")

    return config


def audit_log(operation: str):
    """Simple decorator for tracking data operations."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("audit")
            start_time = datetime.now()

            try:
                result = func(*args, **kwargs)
                logger.info(
                    f"{operation} - {func.__name__} - "
                    f"user={os.getenv('USER', 'system')} - "
                    f"duration={datetime.now() - start_time}"
                )
                return result
            except Exception as e:
                logger.error(
                    f"{operation} FAILED - {func.__name__} - "
                    f"user={os.getenv('USER', 'system')} - "
                    f"error={str(e)}"
                )
                raise

        return wrapper

    return decorator
