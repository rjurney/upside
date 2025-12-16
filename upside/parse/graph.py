"""Graph-based email threading using GraphFrames connected components."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_email_graph(
    input_path: str | Path = "data/parsed_emails.parquet",
    output_path: str | Path = "data/graph_threads.parquet",
    checkpoint_dir: str | Path = "/tmp/graphframes-checkpoint",
) -> Path:
    """Build a graph from email Message-ID and In-Reply-To fields and find connected components.

    Creates a graph where:
    - Vertices are unique message IDs (from both 'id' and 'in_reply_to' fields)
    - Edges go from In-Reply-To (parent) to Message-ID (child)

    Uses GraphFrames connected components to find email thread clusters.

    Parameters
    ----------
    input_path : str or Path
        Path to input parsed emails parquet.
    output_path : str or Path
        Path to output parquet with thread components.
    checkpoint_dir : str or Path
        Directory for GraphFrames checkpointing (required for connected components).

    Returns
    -------
    Path
        Path to output parquet file.
    """
    project_root = Path(__file__).parent.parent.parent
    input_file = project_root / input_path
    output_file = project_root / output_path

    # Create Spark session with GraphFrames package
    spark = (
        SparkSession.builder.appName("EnronEmailGraph")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark3_2.12:0.10.0")
        .getOrCreate()
    )

    # Set checkpoint directory for connected components
    spark.sparkContext.setCheckpointDir(str(checkpoint_dir))

    print(f"Loading parsed emails from {input_file}...")
    emails = spark.read.parquet(str(input_file))

    emails.cache()
    total_count = emails.count()
    print(f"Loaded {total_count:,} emails")

    # Build vertices: all unique message IDs
    # Include both 'id' (Message-ID) and 'in_reply_to' fields
    print("Building vertices from message IDs...")

    # Get IDs from the id column (Message-ID header)
    ids_from_messages = emails.select(F.col("id").alias("id")).filter(F.col("id").isNotNull())

    # Get IDs from in_reply_to column
    ids_from_replies = emails.select(F.col("in_reply_to").alias("id")).filter(
        F.col("in_reply_to").isNotNull()
    )

    # Union and deduplicate to get all unique vertices
    vertices = ids_from_messages.union(ids_from_replies).distinct()
    vertex_count = vertices.count()
    print(f"Found {vertex_count:,} unique message IDs (vertices)")

    # Build edges: in_reply_to -> id relationships (parent -> child)
    print("Building edges from reply relationships...")
    edges = (
        emails.select(F.col("in_reply_to").alias("src"), F.col("id").alias("dst"))
        .filter(F.col("src").isNotNull())
        .filter(F.col("dst").isNotNull())
    )

    edge_count = edges.count()
    print(f"Found {edge_count:,} reply relationships (edges)")

    # Import GraphFrame (must be after SparkSession creation with package)
    from graphframes import GraphFrame

    # Create GraphFrame
    print("Creating GraphFrame...")
    graph = GraphFrame(vertices, edges)

    # Run connected components
    print("Running connected components algorithm...")
    components = graph.connectedComponents()

    # Count unique components
    component_count = components.select("component").distinct().count()
    print(f"Found {component_count:,} connected components (thread clusters)")

    # Join components back to original emails
    print("Joining components to emails...")
    emails_with_components = emails.join(
        components.select(F.col("id").alias("join_id"), "component"),
        emails["id"] == F.col("join_id"),
        how="left",
    ).drop("join_id")

    # Rename component to graph_thread_id for clarity
    emails_with_components = emails_with_components.withColumn(
        "graph_thread_id", F.col("component")
    ).drop("component")

    # Save results
    print(f"Writing results to {output_file}...")
    emails_with_components.write.mode("overwrite").parquet(str(output_file))

    result_count = emails_with_components.count()
    print(f"Saved {result_count:,} emails with graph thread IDs to {output_file}")

    spark.stop()

    return output_file
