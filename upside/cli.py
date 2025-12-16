"""Command line interface for upside."""

import click

from upside.download import download_enron_dataset
from upside.parse.load import load_emails


@click.group()
def main() -> None:
    """Upside - Enron Email Analysis Tool."""
    pass


@main.command()
@click.pass_context
def download(ctx: click.Context) -> None:
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
@click.pass_context
def parse(ctx: click.Context, input_path: str, output_path: str, error_path: str) -> None:
    """Parse the Enron emails CSV into Parquet format."""
    click.echo(f"Parsing emails from {input_path}...")
    parsed_output, error_output = load_emails(input_path, output_path, error_path)
    click.echo(f"Parsed emails saved to: {parsed_output}")
    click.echo(f"Error emails saved to: {error_output}")
