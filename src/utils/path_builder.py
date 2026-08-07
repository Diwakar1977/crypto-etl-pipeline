from datetime import datetime, timezone

class PathBuilder:
    """Generate raw and processed storage paths."""
    
    @staticmethod
    def raw_path(dataset_name: str):
        """Build raw NDJSON path."""

        now = datetime.now(timezone.utc)

        return(
            f"raw_data/"
            f"{dataset_name}/"
            f"year={now:%Y}/"
            f"month={now:%m}/"
            f"day={now:%d}/"
            f"run_time={now:%H%M%S}.ndjson"
        )
    
    @staticmethod
    def processed_path(dataset_name: str):
        """Build processed parquet path."""

        now = datetime.now(timezone.utc)

        return(
            f"{dataset_name}/"
            f"year={now:%Y}/"
            f"month={now:%m}/"
            f"day={now:%d}/"
            f"run_time={now:%H%M%S}/"
        )