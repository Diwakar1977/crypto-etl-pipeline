from datetime import datetime, timezone

class EmailTemplate:
    """Generate SNS and SES notification messages."""

    @staticmethod
    def sns_success(job_name: str):
        """SNS success notification."""

        run_date = datetime.now(
            timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

        subject = (f"SUCCESS | {job_name}")

        message = (
            f"Pipeline : {job_name}\n"
            f"Status   : SUCCESS\n"
            f"Run Date : {run_date}\n\n"
            f"Pipeline completed successfully." 
        )

        return subject, message
    
    @staticmethod
    def sns_failure(job_name: str, stage: str, error: str):
        """SNS failure notification."""
        
        run_date = datetime.now(
            timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

        subject = (f"FAILED | {job_name}")

        message = (
            f"Pipeline : {job_name}\n"
            f"Status   : FAILED\n"
            f"Stage    : {stage}\n"
            f"Run Date : {run_date}\n\n"
            f"Error    : \n"
            f"{error}"
        )

        return subject, message
    