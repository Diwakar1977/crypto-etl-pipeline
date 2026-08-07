import logging
import os
import sys

class Logger:
    """Application logger utility."""

    LOG_DIR = "logs"

    os.makedirs(LOG_DIR, exist_ok=True)

    @staticmethod
    def get_logger(name: str, log_file: str):
        """
        Create and return logger instance.
        """

        logger = logging.getLogger(name)

        if logger.handlers:
            return logger

        logger.setLevel(logging.INFO)
        logger.propagate = False

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

        # File handler
        file_path = os.path.join(
            Logger.LOG_DIR,
            log_file
        )

        file_handler = logging.FileHandler(
            file_path,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

        # Console handler
        # Only add when a valid stderr stream is available.
        if sys.stderr is not None:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    @staticmethod
    def log_banner(logger, message: str):
        """Print formatted banner message."""

        logger.info("=" * 60)
        logger.info(message)
        logger.info("=" * 60)