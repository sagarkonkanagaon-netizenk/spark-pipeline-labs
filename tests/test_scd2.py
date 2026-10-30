from datetime import datetime

from labs.scd2 import apply_scd2

SCHEMA = "customer_id string, city string, tier string, updated_at timestamp"
TRACKED = ["city", "tier"]


def _cdc(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def _dim(spark, path):
    return spark.read.format("delta").load(path)


def test_change_expires_old_row_and_inserts_new_one(spark, tmp_path):
    path = str(tmp_path / "dim_customer")
    t0, t1 = datetime(2026, 1, 1), datetime(2026, 6, 1)

    apply_scd2(
        spark,
        path,
        _cdc(spark, [("c1", "Pune", "gold", t0), ("c2", "Delhi", "silver", t0)]),
        "customer_id",
        TRACKED,
    )
    apply_scd2(
        spark,
        path,
        _cdc(spark, [("c1", "Bengaluru", "gold", t1), ("c3", "Mumbai", "bronze", t1)]),
        "customer_id",
        TRACKED,
    )

    dim = _dim(spark, path)
    c1 = {r.city: r for r in dim.where("customer_id = 'c1'").collect()}
    assert set(c1) == {"Pune", "Bengaluru"}
    assert c1["Pune"].is_current is False and c1["Pune"].valid_to == t1
    assert c1["Bengaluru"].is_current is True and c1["Bengaluru"].valid_from == t1
    assert dim.where("customer_id = 'c3' AND is_current").count() == 1

    # exactly one current row per key
    assert dim.where("is_current").groupBy("customer_id").count().where("count > 1").count() == 0


def test_rerunning_the_same_batch_is_a_no_op(spark, tmp_path):
    path = str(tmp_path / "dim_customer")
    t0, t1 = datetime(2026, 1, 1), datetime(2026, 6, 1)
    apply_scd2(spark, path, _cdc(spark, [("c1", "Pune", "gold", t0)]), "customer_id", TRACKED)

    batch = _cdc(spark, [("c1", "Bengaluru", "gold", t1)])
    apply_scd2(spark, path, batch, "customer_id", TRACKED)
    before = sorted(map(tuple, _dim(spark, path).collect()))
    apply_scd2(spark, path, batch, "customer_id", TRACKED)
    after = sorted(map(tuple, _dim(spark, path).collect()))

    assert before == after
    assert len(after) == 2


def test_multiple_changes_in_one_batch_keep_the_latest(spark, tmp_path):
    path = str(tmp_path / "dim_customer")
    t0 = datetime(2026, 1, 1)
    apply_scd2(spark, path, _cdc(spark, [("c1", "Pune", "gold", t0)]), "customer_id", TRACKED)
    apply_scd2(
        spark,
        path,
        _cdc(
            spark,
            [
                ("c1", "Delhi", "gold", datetime(2026, 2, 1)),
                ("c1", "Chennai", "gold", datetime(2026, 3, 1)),
            ],
        ),
        "customer_id",
        TRACKED,
    )
    current = _dim(spark, path).where("is_current").collect()
    assert [r.city for r in current] == ["Chennai"]
