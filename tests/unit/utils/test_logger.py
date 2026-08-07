import logging
from src.utils.logger import Logger

def test_get_logger():
    logger = Logger.get_logger("test_logger", "test.log")

    assert logger is not None
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"

def test_logger_has_handlers():
    logger = Logger.get_logger("test_logger_handlers", "test.log")

    assert len(logger.handlers) > 0

def test_log_banner(tmp_path):
    log_file = tmp_path / "banner.log"

    logger = Logger.get_logger("banner_logger", str(log_file))

    Logger.log_banner(logger, "CRYPTO ETL PIPELINE STARTED")

    assert log_file.exists()

    content = log_file.read_text()

    assert "CRYPTO ETL PIPELINE STARTED" in content

def test_logger_writes_messages(tmp_path):
    log_file = tmp_path / "messages.log"

    logger = Logger.get_logger("message_logger", str(log_file))

    logger.info("Extracting crypto data")
    logger.warning("API response slow")
    logger.error("API connection failed")

    content = log_file.read_text()

    assert "Extracting crypto data" in content
    assert "API response slow" in content
    assert "API connection failed" in content