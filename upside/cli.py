"""Command line interface for upside."""

import click

from upside.download import download_enron_dataset
from upside.parse.graph import build_email_graph
from upside.parse.load import load_emails
from upside.parse.thread import thread_emails


class OrderedGroup(click.Group):
    """A Click group that preserves command definition order."""

    def list_commands(self, ctx: click.Context) -> list[str]:
        """Return commands in definition order."""
        return list(self.commands.keys())


@click.group(cls=OrderedGroup)
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
    default="data/threaded_emails.parquet",
    help="Path to output Parquet file with threaded emails.",
)
def thread(input_path: str, output_path: str) -> None:
    """Thread emails using the JWZ threading algorithm."""
    click.echo(f"Threading emails from {input_path}...")
    output = thread_emails(input_path, output_path)
    click.echo(f"Threaded emails saved to: {output}")


@main.group(cls=OrderedGroup)
def dev() -> None:
    """[WIP] Development and experimental commands."""
    pass


@dev.command()
@click.option(
    "--input",
    "input_path",
    default="data/parsed_emails.parquet",
    help="Path to input Parquet file with parsed emails.",
)
@click.option(
    "--output",
    "output_path",
    default="data/graph_threads.parquet",
    help="Path to output Parquet file with graph thread IDs.",
)
@click.option(
    "--checkpoint",
    "checkpoint_dir",
    default="/tmp/graphframes-checkpoint",
    help="Directory for GraphFrames checkpointing.",
)
def components(input_path: str, output_path: str, checkpoint_dir: str) -> None:
    """Find email threads using GraphFrames connected components."""
    click.echo(f"Building email graph from {input_path}...")
    output = build_email_graph(input_path, output_path, checkpoint_dir)
    click.echo(f"Graph threads saved to: {output}")
