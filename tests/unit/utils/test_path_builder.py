from src.utils.path_builder import PathBuilder

def test_raw_path():
    """Test raw path generation."""

    path = PathBuilder.raw_path("crypto")

    assert path.startswith("raw_data/crypto/")
    assert "year=" in path
    assert "month=" in path
    assert "day=" in path
    assert "run_time=" in path
    assert path.endswith(".ndjson")


def test_processed_path():
    """Test processed path generation."""

    path = PathBuilder.processed_path("crypto")

    assert path.startswith("crypto/")
    assert "year=" in path
    assert "month=" in path
    assert "day=" in path
    assert "run_time=" in path
    assert path.endswith("/")