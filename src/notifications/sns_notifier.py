import boto3
from botocore.exceptions import ClientError, BotoCoreError

from config.config import Config
from src.utils.logger import Logger

logger = Logger.get_logger("sns_notifier", "notification.log")

class SNSNotifier:
    """Send technical alerts using Amazon SNS."""

    def __init__(
        self,
        topic_arn: str = Config.SNS_TOPIC_ARN,
        region: str = Config.AWS_REGION
    ):
        """Initialize SNS client."""

        try:

            self.topic_arn = topic_arn
            self.region = region

            self.client = boto3.client("sns", region_name=self.region)

            logger.info("SNS notifier initialized successfully.")

        except Exception as e:
            logger.exception(f"Failed to initialize SNS client: {e}")
            raise

    def validate_topic(self):
        """Validate SNS Topic."""

        try:

            if not self.topic_arn:
                raise ValueError("SNS Topic ARN cannot be empty.")

            logger.info("SNS topic validated successfully.")

        except Exception as e:
            logger.exception(f"SNS topic validation failed: {e}")
            raise

    def publish(
        self,
        subject: str,
        message: str
    ):
        """Publish notification."""

        try:

            Logger.log_banner(logger, "SNS NOTIFICATION")

            self.validate_topic()

            logger.info("Publishing SNS notification.")

            response = self.client.publish(
                TopicArn=self.topic_arn,
                Subject=subject,
                Message=message
            )

            logger.info("SNS notification sent successfully.")
            logger.info("Message Id : %s", response["MessageId"])

            return response["MessageId"]

        except (ClientError, BotoCoreError) as e:
            logger.exception(f"SNS publish failed: {e}")
            raise

        except Exception as e:
            logger.exception(f"Unexpected SNS error: {e}")
            raise