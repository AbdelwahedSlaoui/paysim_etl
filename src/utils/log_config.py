import logging
from pathlib import Path
from datetime import datetime
import sys
import warnings

def setup_logging(run_id: str = None) -> logging.Logger:
    """Configure logging with separate domains for business, Spark, and Prefect logs.

    Args:
        run_id: Optional identifier for the logging session

    Returns:
        Logger: Configured business domain logger
    """
    # Basic setup
    run_id = run_id or datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = Path("logs") / run_id
    log_dir.mkdir(parents=True, exist_ok=True)

    # Suppress warnings
    warnings.filterwarnings("ignore")

    # Common formatter
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    def configure_logger(name: str, include_console: bool = False) -> logging.Logger:
        """Helper to configure individual loggers with consistent settings."""
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        # File handler
        file_handler = logging.FileHandler(log_dir / f"{name}.log")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Optional console handler
        if include_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(console_handler)

        return logger

    # Configure domain loggers
    business_logger = configure_logger("business", include_console=True)
    spark_logger = configure_logger("spark")
    prefect_logger = configure_logger("prefect")

    # Route framework logs to appropriate handlers
    for framework, target in [
        ("py4j", "spark"),
        ("pyspark", "spark"),
        ("prefect", "prefect"),
        ("httpx", "prefect")
    ]:
        framework_logger = logging.getLogger(framework)
        framework_logger.handlers.clear()
        framework_logger.addHandler(
            logging.FileHandler(log_dir / f"{target}.log")
        )

    return business_logger