"""Thread emails using the JWZ threading algorithm."""

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from upside.parse import jwzthreading


def create_jwz_message(row: dict[str, Any]) -> jwzthreading.Message:
    """Create a jwzthreading Message from a row dict.

    Parameters
    ----------
    row : dict
        Row from the parsed emails data.

    Returns
    -------
    jwzthreading.Message
        Message object for threading.
    """
    msg = jwzthreading.Message()
    msg.message_id = row.get("message_id") or ""
    msg.subject = row.get("normalized_subject") or ""

    # Combine references and in_reply_to for threading
    references = row.get("references") or []
    if references is None:
        references = []
    # Convert pyarrow list to Python list if needed
    if hasattr(references, "as_py"):
        references = references.as_py() or []
    references = list(references)

    in_reply_to = row.get("in_reply_to")
    if in_reply_to and in_reply_to not in references:
        references = [in_reply_to] + references

    msg.references = references

    # Store the original row data in the message field for later retrieval
    msg.message = row

    return msg


def flatten_thread_tree(
    container: jwzthreading.Container,
    thread_id: str,
    depth: int = 0,
    parent_message_id: str | None = None,
) -> list[dict[str, Any]]:
    """Flatten a thread tree into a list of records with hierarchy info.

    Parameters
    ----------
    container : jwzthreading.Container
        Root container of the thread tree.
    thread_id : str
        ID of the thread (root message_id or subject hash).
    depth : int
        Current depth in the thread tree.
    parent_message_id : str or None
        Message ID of the parent message.

    Returns
    -------
    list[dict]
        List of records with original fields plus threading info.
    """
    results = []

    # Get the message data if this container has one
    if container.message is not None:
        row_data = container.message.message
        if row_data is not None:
            record = dict(row_data)
            record["jwz_thread_id"] = thread_id
            record["thread_depth"] = depth
            record["parent_message_id"] = parent_message_id
            results.append(record)
            parent_message_id = record.get("message_id")

    # Process children
    for child in container.children:
        results.extend(flatten_thread_tree(child, thread_id, depth + 1, parent_message_id))

    return results


def thread_emails_local(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Thread a list of email rows using JWZ algorithm.

    Parameters
    ----------
    rows : list[dict]
        List of email row dictionaries.

    Returns
    -------
    list[dict]
        List of rows with threading fields added.
    """
    if not rows:
        return []

    # Create JWZ messages
    messages = [create_jwz_message(row) for row in rows]

    # Run JWZ threading algorithm
    subject_table = jwzthreading.thread(messages)

    # Flatten all thread trees
    results = []
    for subject, container in subject_table.items():
        # Use the root message_id as thread_id, or hash the subject
        thread_id = None
        if container.message and container.message.message_id:
            thread_id = container.message.message_id
        else:
            # Find first message_id in the tree
            for child in container.children:
                if child.message and child.message.message_id:
                    thread_id = child.message.message_id
                    break

        if thread_id is None:
            thread_id = f"subj:{hash(subject) & 0xFFFFFFFF:08x}"

        results.extend(flatten_thread_tree(container, thread_id))

    return results


def thread_emails(
    input_path: str | Path = "data/parsed_emails.parquet",
    output_path: str | Path = "data/threaded_emails.parquet",
) -> Path:
    """Load parsed emails, thread them using JWZ algorithm, and save.

    Parameters
    ----------
    input_path : str or Path
        Path to input parsed emails parquet.
    output_path : str or Path
        Path to output threaded emails parquet.

    Returns
    -------
    Path
        Path to output parquet file.
    """
    project_root = Path(__file__).parent.parent.parent
    input_file = project_root / input_path
    output_file = project_root / output_path

    print(f"Loading parsed emails from {input_file}...")

    # Read parquet using PyArrow
    table = pq.read_table(input_file)
    total_count = table.num_rows
    print(f"Loaded {total_count:,} emails")

    # Convert to list of dicts
    print("Converting to records...")
    rows = table.to_pylist()

    print("Running JWZ threading algorithm...")
    threaded_rows = thread_emails_local(rows)

    print(f"Threaded {len(threaded_rows):,} emails")

    # Count unique threads
    thread_ids = set(row["jwz_thread_id"] for row in threaded_rows)
    print(f"Found {len(thread_ids):,} unique threads")

    # Save as parquet using PyArrow
    print(f"Writing threaded emails to {output_file}...")
    import pyarrow as pa

    output_table = pa.Table.from_pylist(threaded_rows)
    pq.write_table(output_table, output_file)
    print(f"Saved {len(threaded_rows):,} threaded emails to {output_file}")

    return output_file
