import logging
import os

class Logger:
    """Application logger utility."""

    LOG_DIR = "logs"

    # Create logs directory
    os.makedirs(LOG_DIR, exist_ok=True)

    @staticmethod
    def get_logger(name: str, log_file: str):
        """
        Create and return logger instance.

        Args:
            name: Logger name
            log_file: Log file name

        Returns:
            logging.Logger
        """

        logger = logging.getLogger(name)

        if not logger.handlers:
            logger.setLevel(logging.INFO)
            logger.propagate = False

            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
            )

            file_path = os.path.join(
                Logger.LOG_DIR,
                log_file
            )

            # File handler
            file_handler = logging.FileHandler(
                file_path,
                encoding="utf-8"
            )
            file_handler.setFormatter(formatter)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        return logger


    @staticmethod
    def log_banner(logger, message: str):
        """
        Print formatted banner message.
        """

        logger.info("=" * 60)
        logger.info(message)
        logger.info("=" * 60)