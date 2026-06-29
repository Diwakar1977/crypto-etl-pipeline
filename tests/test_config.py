from config.config import *
from src.utils.logger import get_logger

logger = get_logger(__name__)

print(AWS_REGION)
print(AWS_BUCKET)

logger.info("Test configuration")
