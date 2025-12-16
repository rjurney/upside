# Upside - Enron Email Parser & Thread Analyzer

A PySpark-based pipeline for parsing, normalizing, and threading the Enron email corpus.

## Overview

This project extracts and normalizes emails from the Enron email dataset, implements threading algorithms to group related emails into conversations, and provides graph-based analysis using connected components. The pipeline processes approximately 517,000 messages and outputs structured data in Parquet format.

## Features

- **Email Extraction & Normalization**: Transforms raw email data into structured records

  - Extracts message metadata (id, date, subject, from, to, cc, bcc)
  - Normalizes email addresses to lowercase
  - Parses email headers using the `mail-parser` library
  - Handles malformed multi-line CSV records

- **Email Threading**: Groups related messages into conversation threads

  - JWZ threading algorithm (vendored Python 3 compatible implementation)
  - Uses In-Reply-To and References headers
  - Falls back to normalized subject heuristics

- **Graph Analysis** (WIP): Connected components for thread clustering
  - GraphFrames-based graph construction
  - Message-ID to In-Reply-To edge relationships

## Requirements

- Python 3.12
- Poetry (for dependency management)
- Java 11+ (for PySpark)
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

This downloads the Enron email dataset from Kaggle to `data/emails.csv`.

## Usage

### Run the full pipeline

```bash
# 1. Download the dataset
upside download

# 2. Parse emails into structured Parquet
upside parse

# 3. Thread emails using JWZ algorithm
upside thread
```

### Experimental

```bash
# Find email threads using GraphFrames connected components (WIP)
upside dev spark
```

### CLI Commands

```
Commands:
  download  Download the Enron email dataset from Kaggle.
  parse     Parse the Enron emails CSV into Parquet format.
  thread    Thread emails using the JWZ threading algorithm.
  spark     [WIP] Find email threads using GraphFrames connected components.
```

Run `upside --help` for more information on available commands and options.

### Command Details

#### `upside download`

Downloads the Enron email dataset from Kaggle to `data/emails.csv`.

#### `upside parse`

Parses the raw CSV and extracts structured email fields.

```bash
upside parse --input data/emails.csv --output data/parsed_emails.parquet --error data/error_emails.parquet
```

- Successfully parsed emails go to `parsed_emails.parquet` (partitioned by user)
- Emails that fail to parse go to `error_emails.parquet` with error details

#### `upside thread`

Threads emails using the JWZ algorithm.

```bash
upside thread --input data/parsed_emails.parquet --output data/threaded_emails.parquet
```

Adds threading fields: `jwz_thread_id`, `thread_depth`, `parent_id`

#### `upside spark` (WIP)

Finds email threads using GraphFrames connected components.

```bash
upside spark --input data/parsed_emails.parquet --output data/graph_threads.parquet
```

## Output Schema

### Parsed Emails (`parsed_emails.parquet`)

| Field                | Type          | Description                              |
| -------------------- | ------------- | ---------------------------------------- |
| `file`               | string        | Original file path in Enron dataset      |
| `id`                 | string        | Message-ID header                        |
| `date`               | timestamp     | Email timestamp                          |
| `subject`            | string        | Email subject (original casing)          |
| `from`               | string        | Sender email address (lowercase)         |
| `from_name`          | string        | Sender display name                      |
| `to`                 | array[string] | Recipient email addresses (lowercase)    |
| `cc`                 | array[string] | CC recipients (lowercase)                |
| `bcc`                | array[string] | BCC recipients (lowercase)               |
| `body_clean`         | string        | Plain text message body                  |
| `references`         | array[string] | References header message IDs            |
| `in_reply_to`        | string        | In-Reply-To header message ID            |
| `normalized_subject` | string        | Subject with Re:/Fwd: prefixes removed   |
| `thread_id`          | string        | Thread ID (from headers or subject hash) |
| `user`               | string        | User extracted from file path            |

### Threaded Emails (`threaded_emails.parquet`)

Includes all fields from parsed emails plus:

| Field           | Type   | Description                     |
| --------------- | ------ | ------------------------------- |
| `jwz_thread_id` | string | Thread ID from JWZ algorithm    |
| `thread_depth`  | int    | Depth in thread tree (0 = root) |
| `parent_id`     | string | Parent message ID in thread     |

## Architecture

### Data Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kaggle    │────▶│  emails.csv │────▶│   PySpark   │────▶│  Parquet    │
│   Dataset   │     │  (raw CSV)  │     │   Parser    │     │  (parsed)   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                                                                   ▼
                                        ┌─────────────┐     ┌─────────────┐
                                        │  Parquet    │◀────│     JWZ     │
                                        │ (threaded)  │     │  Threading  │
                                        └─────────────┘     └─────────────┘
```

### Parsing Strategy

The Enron CSV has malformed multi-line message fields. The parser uses:

1. Regex pattern matching to identify record boundaries (`"file_path","Message-ID:`)
2. PySpark window functions with cumulative sum to assign record IDs
3. Line aggregation to reconstruct complete messages
4. `mail-parser` library for header extraction

### Threading Algorithms

**JWZ Threading** (implemented):

- Based on Jamie Zawinski's threading algorithm
- Builds a tree structure from References/In-Reply-To headers
- Groups messages by normalized subject as fallback
- Vendored Python 3 compatible implementation

**GraphFrames Connected Components** (WIP):

- Vertices: unique message IDs
- Edges: id → in_reply_to relationships
- Connected components identify thread clusters

## Project Structure

```
upside/
├── upside/
│   ├── cli.py                 # Command-line interface
│   ├── download.py            # Kaggle dataset download
│   └── parse/
│       ├── __init__.py
│       ├── load.py            # CSV parsing with PySpark
│       ├── extract.py         # Email field extraction
│       ├── thread.py          # JWZ threading
│       ├── jwzthreading.py    # Vendored JWZ library (Python 3)
│       └── graph.py           # GraphFrames connected components
├── tests/                     # Test suite
├── data/                      # Dataset storage (gitignored)
├── pyproject.toml             # Poetry configuration
└── README.md
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Quality

Setup pre-commit hooks:

```bash
pre-commit install
```

Run code quality checks:

```bash
# Run all checks
pre-commit

# Individual tools
black upside tests
isort upside tests
flake8
mypy
```

### Technologies

- **Python 3.12**: Core language
- **PySpark 3.5**: Distributed data processing
- **PyArrow**: Parquet I/O for threading
- **mail-parser**: Email header parsing
- **GraphFrames**: Graph analytics (WIP)
- **Poetry**: Dependency management
- **Click**: Command-line interface

## Scaling Considerations

### CSV Parsing

The global window for record boundary detection is expensive but acceptable for 517K records. For larger datasets:

- Write a proper [Hadoop InputFormat](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/InputFormat.html) to split on record boundaries. Spark builds on Hadoop InputFormats.
- Pre-split the CSV file with overlap and handle bad records at chunk boundaries.
- Use line numbering (`cat -n`) with overlap-aware splitting. Works well: cat -n data/emails.csv | sed 's/^[ \t]_\([0-9]_\)[ \t]\*/\1,/' > /tmp/cat_emails.csv

### JWZ Threading

Currently collects all emails to driver memory. For larger datasets... I don't understand the algorithm well enough yet to shard it properly. Connected components is part of it, and GraphFrames scales that well. Then some graph database could be updated incrementally as new emails arrive. I usually avoid graph databases when building from scratch, but they seem appropriate here.

### Production Pipeline

For 10M+ emails/day:

- Use PySpark Streaming with mini-batches
- Store threads in a graph database (Neo4j, Neptune)
- Update thread relationships incrementally as new emails arrive

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
