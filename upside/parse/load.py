"""Load and parse the malformed Enron emails CSV file."""

import re
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

# Pattern to match the start of a new email record:
# "file_path","Message-ID: ...
RECORD_START_PATTERN = re.compile(r'^"([^"]+)","(Message-ID: .*)$')


def parse_emails_csv(file_path: str) -> Iterator[tuple[str, str]]:
    """Parse the malformed Enron emails CSV file.

    The CSV has a header of "file","message" where the message field spans
    multiple lines. Each record starts with a pattern like:
    "allen-p/_sent_mail/1.","Message-ID: <...>

    Parameters
    ----------
    file_path : str
        Path to the emails.csv file.

    Yields
    ------
    tuple[str, str]
        Tuples of (file_path, message_content).
    """
    current_file: str | None = None
    current_lines: list[str] = []

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        # Skip header line
        header = f.readline()
        if not header.startswith('"file","message"'):
            raise ValueError(f"Unexpected header: {header}")

        for line in f:
            # Check if this line starts a new record
            match = RECORD_START_PATTERN.match(line)

            if match:
                # Yield the previous record if we have one
                if current_file is not None:
                    message = "\n".join(current_lines)
                    # Remove trailing quote if present
                    if message.endswith('"'):
                        message = message[:-1]
                    yield (current_file, message.strip())

                # Start new record
                current_file = match.group(1)
                current_lines = [match.group(2)]
            else:
                # Continue accumulating lines for current record
                current_lines.append(line.rstrip("\n"))

        # Yield the last record
        if current_file is not None:
            message = "\n".join(current_lines)
            if message.endswith('"'):
                message = message[:-1]
            yield (current_file, message.strip())


def load_emails(
    input_path: str = "data/emails.csv", output_path: str = "data/emails.parquet"
) -> str:
    """Load emails from CSV and save as Parquet.

    Parameters
    ----------
    input_path : str
        Path to the input emails.csv file.
    output_path : str
        Path to save the output Parquet file.

    Returns
    -------
    str
        Path to the output Parquet file.
    """
    # Resolve paths relative to project root
    project_root = Path(__file__).parent.parent.parent
    input_file = project_root / input_path
    output_file = project_root / output_path

    # Parse the CSV file and collect records
    print("Parsing emails...")
    files: list[str] = []
    messages: list[str] = []

    for i, (file, message) in enumerate(parse_emails_csv(str(input_file))):
        files.append(file)
        messages.append(message)
        if (i + 1) % 100000 == 0:
            print(f"  Parsed {i + 1:,} emails...")

    print(f"  Total: {len(files):,} emails parsed")

    # Create PyArrow table
    print("Creating Parquet file...")
    table = pa.table({"file": files, "message": messages})

    # Write to Parquet
    pq.write_table(table, str(output_file))

    print(f"Loaded {len(files):,} emails to {output_file}")

    return str(output_file)
