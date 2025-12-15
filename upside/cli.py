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
    default="data/emails.parquet",
    help="Path to output Parquet file.",
)
@click.pass_context
def parse(ctx: click.Context, input_path: str, output_path: str) -> None:
    """Parse the Enron emails CSV into Parquet format."""
    click.echo(f"Parsing emails from {input_path}...")
    output = load_emails(input_path, output_path)
    click.echo(f"Emails saved to: {output}")
