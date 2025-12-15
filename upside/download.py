"""Download Enron email dataset from Kaggle."""

import shutil
from pathlib import Path

import kagglehub


def download_enron_dataset() -> str:
    """Download the Enron email dataset from Kaggle.

    Downloads the latest version of the Enron email dataset from:
    https://www.kaggle.com/datasets/wcukierski/enron-email-dataset

    The emails.csv file is extracted and moved to data/emails.csv in the
    project root.

    Returns
    -------
    str
        Path to the emails.csv file in the data/ directory.
    """
    # Download to kagglehub's default cache location
    kaggle_path = Path(kagglehub.dataset_download("wcukierski/enron-email-dataset"))

    # Define the target data directory
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    target_path = data_dir / "emails.csv"

    # Create data directory if it doesn't exist
    data_dir.mkdir(exist_ok=True)

    # Find and move the emails.csv file
    source_csv = kaggle_path / "emails.csv"
    if not source_csv.exists():
        raise FileNotFoundError(f"emails.csv not found in {kaggle_path}")

    # Remove existing file if present
    if target_path.exists():
        target_path.unlink()

    # Move the CSV file to data/
    shutil.move(str(source_csv), str(target_path))

    # Clean up the downloaded folder
    shutil.rmtree(kaggle_path)

    return str(target_path)
