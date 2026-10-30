"""LAB 02: SCD Type 2 dimension from a CDC feed, in one Delta MERGE.

Changed keys are staged twice: once with their key (matches the current row and
expires it) and once with a NULL merge key (never matches, so it is inserted as
the new current version). Unchanged keys match but fail the hash condition, so a
rerun of the same batch is a no-op.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def latest_per_key(cdc: DataFrame, key: str, order_col: str) -> DataFrame:
    """Keep only the newest change per key, so one batch can't insert two versions."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return cdc.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def with_hash(df: DataFrame, tracked: list[str]) -> DataFrame:
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *cols), 256))


def apply_scd2(
    spark: SparkSession,
    dim_path: str,
    cdc: DataFrame,
    key: str,
    tracked: list[str],
    changed_at: str = "updated_at",
) -> None:
    updates = with_hash(latest_per_key(cdc, key, changed_at), tracked)
    columns = [key, *tracked]

    if not DeltaTable.isDeltaTable(spark, dim_path):
        (
            updates.select(
                *columns,
                "row_hash",
                F.col(changed_at).alias("valid_from"),
                F.lit(None).cast("timestamp").alias("valid_to"),
                F.lit(True).alias("is_current"),
            )
            .write.format("delta")
            .save(dim_path)
        )
        return

    dim = DeltaTable.forPath(spark, dim_path)
    current = dim.toDF().where("is_current")

    changed = (
        updates.alias("u")
        .join(current.alias("c"), key)
        .where("u.row_hash <> c.row_hash")
        .select("u.*")
    )
    staged = updates.withColumn("merge_key", F.col(key)).unionByName(
        changed.withColumn("merge_key", F.lit(None).cast(updates.schema[key].dataType))
    )

    insert_values = {c: f"s.{c}" for c in columns}
    insert_values.update(
        {
            "row_hash": "s.row_hash",
            "valid_from": f"s.{changed_at}",
            "valid_to": "CAST(NULL AS TIMESTAMP)",
            "is_current": "true",
        }
    )

    (
        dim.alias("t")
        .merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "valid_to": f"s.{changed_at}"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
