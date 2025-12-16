"""Command line interface for upside."""

import click

from upside.download import download_enron_dataset
from upside.parse.load import load_emails


@click.group()
def main() -> None:
    """Upside - Enron Email Analysis Tool."""
    pass


@main.command()
def download() -> None:
    """Download the Enron email dataset from Kaggle."""
    click.echo("Downloading Enron email dataset from Kaggle...")
    path = download_enron_dataset()
    click.echo(f"Path to dataset files: {path}")


@main.command()
@click.option(
    "--input",
    "input_path",
    default="data/emails.csv",
    help="Path to input CSV file.",
)
@click.option(
    "--output",
    "output_path",
    default="data/parsed_emails.parquet",
    help="Path to output Parquet file for successfully parsed emails.",
)
@click.option(
    "--error",
    "error_path",
    default="data/error_emails.parquet",
    help="Path to output Parquet file for emails that failed to parse.",
)
def parse(input_path: str, output_path: str, error_path: str) -> None:
    """Parse the Enron emails CSV into Parquet format."""
    click.echo(f"Parsing emails from {input_path}...")
    parsed_output, error_output = load_emails(input_path, output_path, error_path)
    click.echo(f"Parsed emails saved to: {parsed_output}")
    click.echo(f"Error emails saved to: {error_output}")


@main.command()
@click.option(
    "--input",
    "input_path",
    default="data/parsed_emails.parquet",
    help="Path to input Parquet file with parsed emails.",
)
@click.option(
    "--output",
    "output_path",
    default="data/email_threads.parquet",
    help="Path to output Parquet file with email threads.",
)
def threads(input_path: str, output_path: str) -> None:
    """Generate email threads from parsed emails."""
    click.echo(f"Generating email threads from {input_path}...")
    # Placeholder for thread generation logic
    click.echo(f"Email threads saved to: {output_path}")
