import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from src.notifications.sns_notifier import SNSNotifier

@patch("src.notifications.sns_notifier.boto3.client")
def test_publish_success(mock_boto_client):
    """Test successful SNS publish."""

    mock_client = MagicMock()
    mock_client.publish.return_value = {
        "MessageId": "123456789"
    }
    mock_boto_client.return_value = mock_client

    notifier = SNSNotifier(
        topic_arn="arn:aws:sns:ap-south-1:123456789012:test-topic",
        region="ap-south-1",
    )

    message_id = notifier.publish(
        subject="Test Subject",
        message="Test Message",
    )

    assert message_id == "123456789"

    mock_boto_client.assert_called_once_with(
        "sns",
        region_name="ap-south-1",
    )

    mock_client.publish.assert_called_once_with(
        TopicArn="arn:aws:sns:ap-south-1:123456789012:test-topic",
        Subject="Test Subject",
        Message="Test Message",
    )


@patch("src.notifications.sns_notifier.boto3.client")
def test_publish_client_error(mock_boto_client):
    """Test SNS publish raises ClientError."""

    mock_client = MagicMock()
    mock_client.publish.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "InternalError",
                "Message": "SNS Error",
            }
        },
        operation_name="Publish",
    )

    mock_boto_client.return_value = mock_client

    notifier = SNSNotifier(
        topic_arn="arn:test",
        region="ap-south-1",
    )

    with pytest.raises(ClientError):
        notifier.publish(
            subject="Test Subject",
            message="Test Message",
        )

    mock_boto_client.assert_called_once_with(
        "sns",
        region_name="ap-south-1",
    )

    mock_client.publish.assert_called_once_with(
        TopicArn="arn:test",
        Subject="Test Subject",
        Message="Test Message",
    )