"""
transformer.py / emitter.py の単体テスト
PySpark コード → BigQuery SQL の変換結果を検証する
"""
import unittest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from converter import PySparkToBigQueryTransformer


def sql(code: str, var: str | None = None) -> str:
    """変換結果の SQL を返すショートカット"""
    t = PySparkToBigQueryTransformer()
    return t.convert_single(code, var_name=var).sql


class TestSourceConversion(unittest.TestCase):

    def test_spark_table(self):
        result = sql('df = spark.table("mydb.mytable")')
        self.assertIn("FROM `mydb.mytable`", result)

    def test_spark_read_parquet(self):
        t = PySparkToBigQueryTransformer()
        r = t.convert_single('df = spark.read.parquet("/path/to/data")')
        self.assertTrue(len(r.warnings) > 0)  # 警告が出ること


class TestSelectConversion(unittest.TestCase):

    def test_select_columns(self):
        code = """
df = spark.table("t")
result = df.select("col_a", "col_b")
"""
        result = sql(code)
        self.assertIn("SELECT", result)
        self.assertIn("`col_a`", result)
        self.assertIn("`col_b`", result)

    def test_select_with_alias(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.col("x").alias("renamed"))
"""
        result = sql(code)
        self.assertIn("`x` AS `renamed`", result)

    def test_select_star(self):
        code = """
df = spark.table("t")
result = df.select("*")
"""
        result = sql(code)
        self.assertIn("SELECT", result)


class TestFilterConversion(unittest.TestCase):

    def test_filter_equality(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("status") == "active")
"""
        result = sql(code)
        self.assertIn("WHERE", result)
        self.assertIn("`status` = 'active'", result)

    def test_filter_greater_than(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("age") > 18)
"""
        result = sql(code)
        self.assertIn("`age` > 18", result)

    def test_filter_is_null(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("x").isNull())
"""
        result = sql(code)
        self.assertIn("`x` IS NULL", result)

    def test_filter_is_not_null(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("x").isNotNull())
"""
        result = sql(code)
        self.assertIn("`x` IS NOT NULL", result)

    def test_filter_isin(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("status").isin(["a", "b"]))
"""
        result = sql(code)
        self.assertIn("IN (", result)
        self.assertIn("'a'", result)
        self.assertIn("'b'", result)

    def test_filter_and(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter((F.col("a") > 0) & (F.col("b") < 100))
"""
        result = sql(code)
        self.assertIn("AND", result)

    def test_filter_not(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(~(F.col("deleted") == True))
"""
        result = sql(code)
        self.assertIn("NOT", result)


class TestGroupByConversion(unittest.TestCase):

    def test_groupby_sum(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("region").agg(F.sum("amount").alias("total"))
"""
        result = sql(code)
        self.assertIn("GROUP BY", result)
        self.assertIn("SUM(`amount`)", result)
        self.assertIn("AS `total`", result)

    def test_groupby_count(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("region").agg(F.count("id").alias("cnt"))
"""
        result = sql(code)
        self.assertIn("COUNT(`id`)", result)

    def test_groupby_multiple_aggs(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("a", "b").agg(
    F.sum("x").alias("sx"),
    F.avg("y").alias("ay"),
    F.max("z").alias("mz"),
)
"""
        result = sql(code)
        self.assertIn("SUM(`x`)", result)
        self.assertIn("AVG(`y`)", result)
        self.assertIn("MAX(`z`)", result)
        self.assertIn("GROUP BY", result)

    def test_count_distinct(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("region").agg(F.countDistinct("user_id").alias("uniq_users"))
"""
        result = sql(code)
        self.assertIn("COUNT(DISTINCT", result)


class TestJoinConversion(unittest.TestCase):

    def test_inner_join_using(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.join(df2, on=["id"], how="inner")
"""
        result = sql(code)
        self.assertIn("INNER JOIN", result)
        self.assertIn("USING (id)", result)

    def test_left_join(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.join(df2, on=["id"], how="left")
"""
        result = sql(code)
        self.assertIn("LEFT JOIN", result)

    def test_full_outer_join(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.join(df2, on=["id"], how="full")
"""
        result = sql(code)
        self.assertIn("FULL OUTER JOIN", result)

    def test_cross_join(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.join(df2, how="cross")
"""
        result = sql(code)
        self.assertIn("CROSS JOIN", result)


class TestCaseWhenConversion(unittest.TestCase):

    def test_when_otherwise(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(
    F.when(F.col("score") >= 90, "A")
     .when(F.col("score") >= 70, "B")
     .otherwise("C")
     .alias("grade")
)
"""
        result = sql(code)
        self.assertIn("CASE", result)
        self.assertIn("WHEN", result)
        self.assertIn("THEN", result)
        self.assertIn("ELSE 'C'", result)
        self.assertIn("END", result)

    def test_when_no_otherwise(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.when(F.col("flag") == 1, "yes").alias("check"))
"""
        result = sql(code)
        self.assertIn("CASE", result)
        self.assertNotIn("ELSE", result)


class TestWindowConversion(unittest.TestCase):

    def test_row_number(self):
        code = """
import pyspark.sql.functions as F
from pyspark.sql.window import Window
df = spark.table("t")
w = Window.partitionBy("region").orderBy(F.col("amount").desc())
result = df.withColumn("rn", F.row_number().over(w))
"""
        result = sql(code)
        self.assertIn("ROW_NUMBER()", result)
        self.assertIn("OVER", result)
        self.assertIn("PARTITION BY", result)

    def test_lag(self):
        code = """
import pyspark.sql.functions as F
from pyspark.sql.window import Window
df = spark.table("t")
w = Window.partitionBy("id").orderBy("date")
result = df.withColumn("prev", F.lag("value", 1).over(w))
"""
        result = sql(code)
        self.assertIn("LAG(", result)
        self.assertIn("OVER", result)

    def test_rank(self):
        code = """
import pyspark.sql.functions as F
from pyspark.sql.window import Window
df = spark.table("t")
w = Window.partitionBy("cat").orderBy(F.col("score").desc())
result = df.withColumn("rnk", F.rank().over(w))
"""
        result = sql(code)
        self.assertIn("RANK()", result)


class TestFunctionConversion(unittest.TestCase):

    def test_coalesce(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.coalesce(F.col("a"), F.col("b"), F.lit(0)).alias("c"))
"""
        result = sql(code)
        self.assertIn("COALESCE(", result)

    def test_cast(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.col("amount").cast("double").alias("amt"))
"""
        result = sql(code)
        self.assertIn("CAST(", result)
        self.assertIn("AS FLOAT64", result)

    def test_upper_lower(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.upper(F.col("name")).alias("uname"))
"""
        result = sql(code)
        self.assertIn("UPPER(", result)

    def test_date_add(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.date_add(F.col("dt"), 7).alias("next_week"))
"""
        result = sql(code)
        self.assertIn("DATE_ADD(", result)
        self.assertIn("INTERVAL 7 DAY", result)

    def test_datediff(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.datediff(F.col("end_dt"), F.col("start_dt")).alias("days"))
"""
        result = sql(code)
        self.assertIn("DATE_DIFF(", result)
        self.assertIn("DAY", result)

    def test_year_month_day(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(
    F.year(F.col("dt")).alias("y"),
    F.month(F.col("dt")).alias("m"),
    F.dayofmonth(F.col("dt")).alias("d"),
)
"""
        result = sql(code)
        self.assertIn("EXTRACT(YEAR FROM", result)
        self.assertIn("EXTRACT(MONTH FROM", result)
        self.assertIn("EXTRACT(DAY FROM", result)

    def test_collect_list(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("id").agg(F.collect_list("item").alias("items"))
"""
        result = sql(code)
        self.assertIn("ARRAY_AGG(", result)
        self.assertIn("IGNORE NULLS", result)

    def test_count_star(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("region").agg(F.count("*").alias("cnt"))
"""
        result = sql(code)
        self.assertIn("COUNT(*)", result)


class TestMiscConversion(unittest.TestCase):

    def test_limit(self):
        code = """
df = spark.table("t")
result = df.limit(100)
"""
        result = sql(code)
        self.assertIn("LIMIT 100", result)

    def test_distinct(self):
        code = """
df = spark.table("t")
result = df.distinct()
"""
        result = sql(code)
        self.assertIn("SELECT DISTINCT", result)

    def test_drop(self):
        code = """
df = spark.table("t")
result = df.drop("col1", "col2")
"""
        result = sql(code)
        self.assertIn("SELECT * EXCEPT(", result)
        self.assertIn("`col1`", result)

    def test_union_all(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.union(df2)
"""
        result = sql(code)
        self.assertIn("UNION ALL", result)

    def test_with_column(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.withColumn("doubled", F.col("amount") * 2)
"""
        result = sql(code)
        self.assertIn("`amount` * 2 AS `doubled`", result)

    def test_rename(self):
        code = """
df = spark.table("t")
result = df.withColumnRenamed("old_name", "new_name")
"""
        result = sql(code)
        self.assertIn("`old_name` AS `new_name`", result)

    def test_order_by_desc(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.orderBy(F.col("amount").desc())
"""
        result = sql(code)
        self.assertIn("ORDER BY", result)
        self.assertIn("DESC", result)

    def test_full_pipeline(self):
        """sample_basic.py が正常に変換されること"""
        from pathlib import Path
        code = Path("fixtures/pipelines/sample_basic.py").read_text()
        t = PySparkToBigQueryTransformer()
        r = t.convert_single(code, var_name="result")
        self.assertNotIn("ERROR", r.sql)
        self.assertIn("WITH", r.sql)
        self.assertIn("PARTITION BY", r.sql)
        self.assertIn("ORDER BY", r.sql)
        self.assertIn("GROUP BY", r.sql)


class TestNewOperations(unittest.TestCase):
    """新しく追加した DataFrame 操作のテスト"""

    def test_select_expr(self):
        code = """
df = spark.table("t")
result = df.selectExpr("col1", "col2 as c2", "sum(col3) as total")
"""
        result = sql(code)
        self.assertIn("col1", result)
        self.assertIn("col2 as c2", result)

    def test_fillna_with_dict(self):
        code = """
df = spark.table("t")
result = df.fillna({"col1": 0, "col2": "unknown"})
"""
        result = sql(code)
        self.assertIn("COALESCE(", result)
        self.assertIn("`col1`", result)

    def test_fillna_with_subset(self):
        code = """
df = spark.table("t")
result = df.fillna(0, subset=["amount", "qty"])
"""
        result = sql(code)
        self.assertIn("COALESCE(`amount`", result)
        self.assertIn("COALESCE(`qty`", result)

    def test_na_fill(self):
        code = """
df = spark.table("t")
result = df.na.fill({"status": "active"})
"""
        result = sql(code)
        self.assertIn("COALESCE(", result)
        self.assertIn("`status`", result)

    def test_dropna_any(self):
        code = """
df = spark.table("t")
result = df.dropna(how="any", subset=["col1", "col2"])
"""
        result = sql(code)
        self.assertIn("`col1` IS NOT NULL", result)
        self.assertIn("`col2` IS NOT NULL", result)

    def test_dropna_all(self):
        code = """
df = spark.table("t")
result = df.dropna("all", subset=["col1", "col2"])
"""
        result = sql(code)
        self.assertIn("`col1` IS NULL", result)
        self.assertIn("NOT (", result)

    def test_na_drop(self):
        code = """
df = spark.table("t")
result = df.na.drop(how="any", subset=["col1"])
"""
        result = sql(code)
        self.assertIn("`col1` IS NOT NULL", result)

    def test_intersect(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.intersect(df2)
"""
        result = sql(code)
        self.assertIn("INTERSECT DISTINCT", result)

    def test_subtract(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.subtract(df2)
"""
        result = sql(code)
        self.assertIn("EXCEPT DISTINCT", result)

    def test_cross_join(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.crossJoin(df2)
"""
        result = sql(code)
        self.assertIn("CROSS JOIN", result)

    def test_todf(self):
        code = """
df = spark.table("t")
result = df.toDF("a", "b", "c")
"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_single(code)
        self.assertTrue(len(r.warnings) > 0)

    def test_sample(self):
        code = """
df = spark.table("t")
result = df.sample(0.1)
"""
        result = sql(code)
        self.assertIn("TABLESAMPLE", result)

    def test_replace_dict(self):
        code = """
df = spark.table("t")
result = df.na.replace({"old_val": "new_val"}, subset=["col1"])
"""
        result = sql(code)
        self.assertIn("CASE", result)
        self.assertIn("`col1`", result)

    def test_rollup(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.rollup("region", "status").agg(F.count("*").alias("cnt"))
"""
        result = sql(code)
        self.assertIn("GROUP BY ROLLUP(", result)

    def test_cube(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.cube("region", "status").agg(F.sum("amount").alias("total"))
"""
        result = sql(code)
        self.assertIn("GROUP BY CUBE(", result)

    def test_pivot(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("region").pivot("status", ["active", "cancelled"]).agg(F.sum("amount"))
"""
        result = sql(code)
        self.assertIn("PIVOT", result)
        self.assertIn("`status`", result)

    def test_unpivot(self):
        code = """
df = spark.table("t")
result = df.unpivot(["id"], ["col1", "col2"], "variable", "value")
"""
        result = sql(code)
        self.assertIn("UNPIVOT", result)

    def test_read_format_load(self):
        code = """
df = spark.read.format("parquet").load("/path/to/data")
result = df.select("col1")
"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_single(code)
        self.assertIn("SELECT", r.sql)

    def test_crosstab_warning(self):
        code = """
df = spark.table("t")
result = df.crosstab("col1", "col2")
"""
        t = PySparkToBigQueryTransformer()
        r = t.convert_single(code)
        self.assertTrue(any("crosstab" in w for w in r.warnings))


if __name__ == "__main__":
    unittest.main()
