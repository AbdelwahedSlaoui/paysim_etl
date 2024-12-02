import logging
import warnings
from pathlib import Path
from datetime import datetime
import sys

class DomainLogger:
    """Manages separate logging domains while maintaining coherent structure."""

    def __init__(self, run_id: str = None):
        self.run_id = run_id or datetime.now().strftime('%Y%m%d_%H%M%S')
        self.log_dir = Path("logs") / self.run_id
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Initialize domain loggers
        self.business_logger = self._setup_business_logger()
        self.spark_logger = self._setup_spark_logger()
        self.prefect_logger = self._setup_prefect_logger()

        # Suppress warnings
        warnings.filterwarnings("ignore")

    def _create_file_handler(self, filename: str, formatter: logging.Formatter) -> logging.FileHandler:
        """Creates a file handler with specified formatting."""
        handler = logging.FileHandler(self.log_dir / filename)
        handler.setFormatter(formatter)
        return handler

    def _setup_business_logger(self) -> logging.Logger:
        """Configure business logic logger with console output."""
        logger = logging.getLogger("business")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        # File handler for business logs
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        file_handler = self._create_file_handler("business.log", file_formatter)

        # Console handler for business logs only
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(message)s'))

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    def _setup_spark_logger(self) -> logging.Logger:
        """Configure Spark-specific logger."""
        logger = logging.getLogger("spark")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        handler = self._create_file_handler("spark.log", formatter)
        logger.addHandler(handler)

        # Capture Spark logs
        logging.getLogger("py4j").handlers.clear()
        logging.getLogger("py4j").addHandler(handler)
        logging.getLogger("pyspark").handlers.clear()
        logging.getLogger("pyspark").addHandler(handler)

        return logger

    def _setup_prefect_logger(self) -> logging.Logger:
        """Configure Prefect-specific logger."""
        logger = logging.getLogger("prefect")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler = self._create_file_handler("prefect.log", formatter)
        logger.addHandler(handler)

        # Capture Prefect and related logs
        logging.getLogger("prefect").handlers.clear()
        logging.getLogger("prefect").addHandler(handler)
        logging.getLogger("httpx").handlers.clear()
        logging.getLogger("httpx").addHandler(handler)

        return logger

    def get_logger(self, domain: str = "business") -> logging.Logger:
        """Retrieve the appropriate logger for the specified domain."""
        loggers = {
            "business": self.business_logger,
            "spark": self.spark_logger,
            "prefect": self.prefect_logger
        }
        return loggers.get(domain, self.business_logger)

def setup_logging() -> logging.Logger:
    """Create and configure domain-separated logging infrastructure."""
    domain_logger = DomainLogger()
    return domain_logger.get_logger()