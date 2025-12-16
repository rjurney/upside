# Upside - Enron Email Parser & Thread Analyzer

A PySpark-based pipeline for parsing, normalizing, and threading the Enron email corpus.

## Overview

This project extracts and normalizes emails from the Enron email dataset, including nested and quoted messages, and implements a threading algorithm to group related emails into conversations. The pipeline processes approximately 500,000 messages (~3 GB uncompressed) and outputs structured data in Parquet format.

## Features

- **Email Extraction & Normalization**: Transforms raw email data into structured records

  - Extracts message metadata (id, date, subject, from, to, cc, bcc)
  - Normalizes email addresses to lowercase
  - Removes quoted history to extract clean message bodies
  - Surfaces inline forwards/replies as separate rows

- **Email Threading**: Groups related messages into conversation threads

  - Assigns stable `thread_id` to messages in the same conversation
  - Uses header fields and heuristics for thread detection

- **Scalable Architecture**: Built on PySpark for distributed processing

## Requirements

- Python 3.12
- Poetry (for dependency management)
- ~4 GB disk space for the Enron dataset

## Setup

### 1. Create and activate a Python 3.12 virtual environment

```bash
# Using conda (recommended)
conda create -n upside python=3.12 -y
conda activate upside

# Or using venv
python3.12 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 2. Install dependencies

```bash
poetry install
```

### 3. Download the Enron dataset

```bash
upside download
```

This will download the Enron email dataset from Kaggle to the `data/` directory.

## Usage

### Run the full pipeline

```bash
# Parse emails and extract threads
upside parse

# Build knowledge graph (hope I have time)
upside kg
```

### CLI Commands

- `upside download` - Download the Enron dataset from Kaggle
- `upside parse` - Parse emails and assign thread IDs
- `upside kg` - Build knowledge graph from parsed emails

Run `poetry run upside --help` for more information on available commands.

## Output Schema

The pipeline produces structured data with the following fields:

| Field        | Type          | Description                                              |
| ------------ | ------------- | -------------------------------------------------------- |
| `id`         | string        | Stable message identifier (consistent across executions) |
| `date`       | timestamp     | UTC ISO-8601 timestamp                                   |
| `subject`    | string        | Email subject (original casing)                          |
| `from`       | string        | Sender email address (lowercase)                         |
| `to`         | array[string] | Recipient email addresses (lowercase)                    |
| `cc`         | array[string] | CC recipients (lowercase)                                |
| `bcc`        | array[string] | BCC recipients (lowercase)                               |
| `body_clean` | string        | Message body with quoted history removed                 |
| `thread_id`  | string        | Conversation thread identifier                           |

## Architecture

### Parsing Data Flow

1. **Download**: Fetch Enron email dataset from Kaggle API
2. **Parse**: Extract and normalize emails using PySpark
   - Read CSV files containing raw email data
   - Parse email headers and bodies
   - Detect and extract nested/quoted messages
   - Clean message bodies
3. **Thread**: Assign thread IDs based on conversation relationships
   - Use In-Reply-To and References headers
   - Apply subject-based heuristics for missing headers
4. **Output**: Save structured data in Parquet format

### Technologies

- **Python 3.12**: Core language
- **PySpark**: Distributed data processing
- **Poetry**: Dependency management and packaging
- **Click**: Command-line interface
- **BAML**: LLM-based structured extraction for knowledge graph

## Development

### Running Tests

```bash
# Run all tests
pytest tests/
```

### Code Quality

```bash
# Lint and format
pre-commit
```

Which runs the following checks:

```bash
# Format code
black upside tests
isort upside tests

# Lint code
flake8

# Type checking
mypy
```

## Project Structure

```
upside/
├── upside/                 # Core application code
│   ├── download/          # Dataset download logic
│   ├── parse/             # Email parsing and normalization
│   ├── kg/                # Knowledge graph building
│   └── cli.py             # Command-line interface
├── tests/                 # Test suite
├── data/                  # Dataset storage (gitignored)
├── pyproject.toml         # Poetry configuration
└── README.md              # This file
```

## Scale-Up Parsing Architecture

The file format this is in is let's say... not ideal for distributed processing. I used PySpark to scale up from the start as much as possible - something I've advocated for more than a decade when building things that need to scale. The challenge to parsing the emails is that we need to identify record boundaries before we can partition, but the Window / cumulative sum approach requires global ordering. There's no way around this without pre-splitting the file or doing a two-pass approach.

Here's what (edited by me) Claude has to say:

```
  window = Window.orderBy("line_id")
  lines = lines.withColumn("record_id", F.sum("is_record_start").over(window))

  This is a global window with no partitionBy() - it's required to track record boundaries across the entire CSV file to extract the structure from the wonky CSV. We need cumulative sum across ALL lines to assign record IDs.

  The problem: we don't know which user an email belongs to until AFTER we've:
  1. Identified record boundaries (the "file_path","Message-ID: pattern)
  2. Grouped the multi-line messages together
  3. Extracted the file path
```

A two-pass approach or pre-splitting the file would be most reliable. I would salt the lines or split the file crudely using "Message-ID" as a string, or even using some utility that can split large files using bytes into smaller chunks at line boundaries with overlap. Then each chunk can be processed independently in Spark, and we can partition by user after extracting the file path.

Actually `cat -n` would be blazing fast for line numbering, then split on line numbers with overlap. A simple Python script could do this too. You could build on `cat -n data/emails.csv | sed 's/^[ \t]*\([0-9]*\)[ \t]*/\1,/' > /tmp/cat_emails.csv`.

The global window is expensive but acceptable for 517K records. For 10M+ emails/day (per the assignment), pre-splitting at ingest time would be the way to go, although with PySpark we are talking about PySpark Streaming and mini-batches, something this implementation does not consider. We would need to use a graph database to connect to threaded emails. New content would come from a stream of emails, and we would need to update the threads in the graph database as new emails arrive.

## Time-Boxing Note

This project was developed as a take-home assignment with a 4-hour time limit for coding. Some features may be incomplete or require further refinement. See TODOs in the code and the "Future Work" section above for planned improvements.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
