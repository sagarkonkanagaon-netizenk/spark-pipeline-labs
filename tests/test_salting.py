from pyspark.sql import functions as F

from labs.salting import add_salt, find_hot_keys, salted_join


def _skewed(spark):
    # c_hot owns 900 of 1,000 fact rows
    facts = spark.range(1000).select(
        F.when(F.col("id") < 900, F.lit("c_hot"))
        .otherwise(F.concat(F.lit("c_"), (F.col("id") % 20).cast("string")))
        .alias("customer_id"),
        F.col("id").alias("order_id"),
    )
    dims = spark.createDataFrame(
        [("c_hot", "Hot Seller")] + [(f"c_{i}", f"Seller {i}") for i in range(20)],
        "customer_id string, name string",
    )
    return facts, dims


def test_finds_the_hot_key(spark):
    facts, _ = _skewed(spark)
    assert find_hot_keys(facts, "customer_id", share=0.2) == ["c_hot"]


def test_salted_join_returns_exactly_the_plain_join(spark):
    facts, dims = _skewed(spark)
    plain = facts.join(dims, "customer_id")
    salted = salted_join(facts, dims, "customer_id", ["c_hot"], buckets=8)

    assert salted.count() == plain.count() == 1000
    assert salted.exceptAll(plain).count() == 0
    assert plain.exceptAll(salted).count() == 0


def test_hot_key_is_spread_across_buckets(spark):
    facts, _ = _skewed(spark)
    buckets = 8
    salted = add_salt(facts, "customer_id", ["c_hot"], buckets)
    per_bucket = salted.where("customer_id = 'c_hot'").groupBy("_salt").count().collect()

    assert len(per_bucket) == buckets
    # no bucket holds more than twice its fair share of the hot key
    assert max(r["count"] for r in per_bucket) < 2 * 900 / buckets


def test_cold_keys_are_not_salted(spark):
    facts, _ = _skewed(spark)
    salted = add_salt(facts, "customer_id", ["c_hot"], 8)
    assert salted.where("customer_id <> 'c_hot' AND _salt <> 0").count() == 0
