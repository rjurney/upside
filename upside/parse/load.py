"""Load and parse the malformed Enron emails CSV file using PySpark."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Regex pattern to match the start of a new email record:
# "file_path","Message-ID: ...
RECORD_START_PATTERN = r'^"([^"]+)","(Message-ID: .*)$'


def load_emails_spark(spark: SparkSession, input_path: str) -> DataFrame:
    """Load the malformed Enron emails CSV into a Spark DataFrame.

    The CSV has a header of "file","message" where the message field spans
    multiple lines. Each record starts with a pattern like:
    "allen-p/_sent_mail/1.","Message-ID: <...>

    This function uses PySpark to:
    1. Read the file as text lines
    2. Identify record boundaries using regex
    3. Group lines by record using window functions
    4. Aggregate lines into complete messages

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    input_path : str
        Path to the emails.csv file.

    Returns
    -------
    DataFrame
        Spark DataFrame with columns: file, message
    """
    # Read file as text, preserving line order with monotonically_increasing_id
    lines = spark.read.text(input_path).withColumn("line_id", F.monotonically_increasing_id())

    # Filter out the header line
    lines = lines.filter(~F.col("value").startswith('"file","message"'))

    # Identify lines that start a new record (match the pattern)
    lines = lines.withColumn(
        "is_record_start", F.when(F.col("value").rlike(RECORD_START_PATTERN), 1).otherwise(0)
    )

    # Assign record IDs using cumulative sum of record starts
    # This groups consecutive lines under the same record ID
    window = Window.orderBy("line_id")
    lines = lines.withColumn("record_id", F.sum("is_record_start").over(window))

    # Filter out lines before the first record (record_id = 0 means no record started yet)
    lines = lines.filter(F.col("record_id") > 0)

    # For the first line of each record, extract file path and message start
    # For subsequent lines, the entire line is part of the message
    lines = lines.withColumn(
        "file_path",
        F.when(
            F.col("is_record_start") == 1, F.regexp_extract("value", RECORD_START_PATTERN, 1)
        ).otherwise(None),
    ).withColumn(
        "message_line",
        F.when(
            F.col("is_record_start") == 1, F.regexp_extract("value", RECORD_START_PATTERN, 2)
        ).otherwise(F.col("value")),
    )

    # Group by record_id and aggregate
    # Use first() for file_path (only the first line has it)
    # Use collect_list() ordered by line_id to preserve line order
    window_ordered = Window.partitionBy("record_id").orderBy("line_id")
    lines = lines.withColumn("line_order", F.row_number().over(window_ordered))

    emails = lines.groupBy("record_id").agg(
        F.first("file_path", ignorenulls=True).alias("file"),
        F.concat_ws("\n", F.collect_list(F.col("message_line"))).alias("message_raw"),
    )

    # Clean up the message: remove trailing quote if present, trim whitespace
    emails = emails.withColumn("message", F.trim(F.regexp_replace("message_raw", '"$', ""))).select(
        "file", "message"
    )

    return emails


def load_emails(
    input_path: str = "data/emails.csv", output_path: str = "data/emails.parquet"
) -> str:
    """Load emails from CSV and save as Parquet using PySpark.

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

    # Create Spark session
    spark = (
        SparkSession.builder.appName("EnronEmailParser")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    print(f"Loading emails from {input_file}...")

    # Load and parse emails
    emails = load_emails_spark(spark, str(input_file))

    # Cache for count and write
    emails.cache()

    # Get count
    count = emails.count()
    print(f"Parsed {count:,} emails")

    # Save as Parquet
    print(f"Writing to {output_file}...")
    emails.write.mode("overwrite").parquet(str(output_file))

    print(f"Loaded {count:,} emails to {output_file}")

    spark.stop()

    return str(output_file)
