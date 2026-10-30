"""LAB 03: taming a skewed join with salting.

Only the hot keys are salted: their rows on the large side get a random bucket
0..n-1, and their rows on the small side are replicated n times. Every other key
keeps salt 0, so the small side only grows by (n - 1) rows per hot key.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def find_hot_keys(df: DataFrame, key: str, share: float = 0.2) -> list:
    """Keys that own at least `share` of all rows."""
    total = df.count()
    if total == 0:
        return []
    rows = (
        df.groupBy(key).count().where(F.col("count") >= F.lit(total * share)).select(key).collect()
    )
    return [r[0] for r in rows]


def _is_hot(key: str, hot_keys: list) -> Column:
    return F.col(key).isin(hot_keys) if hot_keys else F.lit(False)


def add_salt(large: DataFrame, key: str, hot_keys: list, buckets: int, seed: int = 7) -> DataFrame:
    """Random bucket 0..buckets-1 for hot-key rows, 0 for everything else."""
    return large.withColumn(
        "_salt",
        F.when(_is_hot(key, hot_keys), (F.rand(seed=seed) * buckets).cast("int")).otherwise(0),
    )


def salted_join(
    large: DataFrame,
    small: DataFrame,
    key: str,
    hot_keys: list,
    buckets: int = 8,
    how: str = "inner",
) -> DataFrame:
    is_hot = _is_hot(key, hot_keys)
    large_salted = add_salt(large, key, hot_keys, buckets)
    # explode can't sit inside when(), so choose the array first, then explode it
    small_salted = small.withColumn(
        "_salt",
        F.explode(
            F.when(is_hot, F.array([F.lit(i) for i in range(buckets)])).otherwise(F.array(F.lit(0)))
        ),
    )
    return large_salted.join(small_salted, [key, "_salt"], how).drop("_salt")
