"""Tests for download module."""

from upside.download import download_enron_dataset


def test_download_enron_dataset_exists() -> None:
    """Test that download_enron_dataset function exists and is documented."""
    assert callable(download_enron_dataset)
    assert download_enron_dataset.__doc__ is not None
