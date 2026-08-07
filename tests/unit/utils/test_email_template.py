from src.utils.email_template import EmailTemplate

def test_sns_success():
    """Test SNS success notification."""

    subject, message = EmailTemplate.sns_success("Crypto ETL")

    assert subject == "SUCCESS | Crypto ETL"
    assert "Pipeline : Crypto ETL" in message
    assert "Status   : SUCCESS" in message
    assert "Run Date :" in message
    assert "Pipeline completed successfully." in message


def test_sns_failure():
    """Test SNS failure notification."""

    subject, message = EmailTemplate.sns_failure(
        job_name="Crypto ETL",
        stage="Transform",
        error="Schema validation failed."
    )

    assert subject == "FAILED | Crypto ETL"
    assert "Pipeline : Crypto ETL" in message
    assert "Status   : FAILED" in message
    assert "Stage    : Transform" in message
    assert "Run Date :" in message
    assert "Error" in message
    assert "Schema validation failed." in message