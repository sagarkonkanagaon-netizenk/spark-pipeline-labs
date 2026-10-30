"""LAB 01: exactly-once output from an at-least-once event stream.

Duplicates are dropped on a stable event id within an event-time watermark, so
Spark keeps a bounded window of ids in state instead of every id ever seen.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("amount", DoubleType(), True),
    ]
)


def dedup_events(events: DataFrame, watermark: str = "2 hours") -> DataFrame:
    """Drop redelivered events. Works on streaming and batch DataFrames."""
    return events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(["event_id"])


def run_dedup(
    spark: SparkSession,
    input_dir: str,
    output_path: str,
    checkpoint_dir: str,
    watermark: str = "2 hours",
) -> None:
    """Process every JSON file currently in input_dir into a clean Delta table.

    Uses trigger(availableNow=True), so the same code serves a scheduled batch run
    and a continuous stream. The checkpoint holds both the processed files and the
    dedup state, which is what makes a replay safe.
    """
    stream = spark.readStream.schema(EVENT_SCHEMA).json(input_dir)
    query = (
        dedup_events(stream, watermark)
        .writeStream.format("delta")
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(output_path)
    )
    query.awaitTermination()
