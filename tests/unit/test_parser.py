"""parser.py の単体テスト (unittest)"""
import unittest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from converter.parser import PySparkParser
from converter.ir import (
    SourceTable, SelectPlan, FilterPlan, GroupByPlan,
    JoinPlan, JoinType, OrderByPlan, LimitPlan, DistinctPlan,
    UnionPlan, WithColumnPlan, DropPlan, RenamePlan,
    ColRef, Literal, Alias, BinaryOp, FunctionCall, CaseWhen,
    IsNull, IsNotNull, InList, Cast,
)


def make_parser():
    return PySparkParser()


class TestSourceParsing(unittest.TestCase):
    def setUp(self):
        self.parser = make_parser()

    def test_spark_table(self):
        plans = self.parser.parse_code('df = spark.table("mydb.mytable")')
        self.assertIn("df", plans)
        src = plans["df"]
        self.assertIsInstance(src, SourceTable)
        self.assertEqual(src.name, "mydb.mytable")
        self.assertEqual(src.source_type, "table")

    def test_spark_read_parquet(self):
        plans = self.parser.parse_code('df = spark.read.parquet("/path/to/data")')
        self.assertIsInstance(plans["df"], SourceTable)
        self.assertEqual(plans["df"].source_type, "parquet")

    def test_spark_read_csv(self):
        plans = self.parser.parse_code('df = spark.read.csv("/path/to/data.csv")')
        self.assertEqual(plans["df"].source_type, "csv")


class TestSelectParsing(unittest.TestCase):
    def setUp(self):
        self.parser = make_parser()

    def test_select_string_cols(self):
        code = """
df = spark.table("t")
result = df.select("col_a", "col_b")
"""
        plans = self.parser.parse_code(code)
        sel = plans["result"]
        self.assertIsInstance(sel, SelectPlan)
        self.assertEqual(sel.columns, (ColRef("col_a"), ColRef("col_b")))

    def test_select_with_f_col(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.col("x"), F.col("y").alias("z"))
"""
        plans = self.parser.parse_code(code)
        sel = plans["result"]
        self.assertIsInstance(sel, SelectPlan)
        self.assertEqual(sel.columns[0], ColRef("x"))
        self.assertIsInstance(sel.columns[1], Alias)
        self.assertEqual(sel.columns[1].name, "z")


class TestFilterParsing(unittest.TestCase):
    def setUp(self):
        self.parser = make_parser()

    def test_filter_equality(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("status") == "active")
"""
        plans = self.parser.parse_code(code)
        flt = plans["result"]
        self.assertIsInstance(flt, FilterPlan)
        self.assertIsInstance(flt.condition, BinaryOp)
        self.assertEqual(flt.condition.op, "=")
        self.assertEqual(flt.condition.left, ColRef("status"))
        self.assertEqual(flt.condition.right, Literal("active"))

    def test_where_alias(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.where(F.col("age") > 18)
"""
        plans = self.parser.parse_code(code)
        self.assertIsInstance(plans["result"], FilterPlan)

    def test_filter_is_null(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("x").isNull())
"""
        plans = self.parser.parse_code(code)
        flt = plans["result"]
        self.assertIsInstance(flt.condition, IsNull)

    def test_filter_isin(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.filter(F.col("status").isin(["a", "b", "c"]))
"""
        plans = self.parser.parse_code(code)
        flt = plans["result"]
        self.assertIsInstance(flt.condition, InList)
        self.assertEqual(len(flt.condition.values), 3)


class TestGroupByParsing(unittest.TestCase):
    def setUp(self):
        self.parser = make_parser()

    def test_groupby_agg(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("region").agg(F.sum("amount").alias("total"))
"""
        plans = self.parser.parse_code(code)
        grp = plans["result"]
        self.assertIsInstance(grp, GroupByPlan)
        self.assertEqual(grp.keys, (ColRef("region"),))
        self.assertEqual(len(grp.aggregations), 1)
        agg = grp.aggregations[0]
        self.assertIsInstance(agg, Alias)
        self.assertEqual(agg.name, "total")

    def test_multi_key_groupby(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.groupBy("a", "b", "c").agg(F.count("*").alias("cnt"))
"""
        plans = self.parser.parse_code(code)
        grp = plans["result"]
        self.assertEqual(len(grp.keys), 3)


class TestJoinParsing(unittest.TestCase):
    def setUp(self):
        self.parser = make_parser()

    def test_inner_join_using(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.join(df2, on=["id"], how="inner")
"""
        plans = self.parser.parse_code(code)
        j = plans["result"]
        self.assertIsInstance(j, JoinPlan)
        self.assertEqual(j.join_type, JoinType.INNER)
        self.assertEqual(j.using_columns, ("id",))

    def test_left_join(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.join(df2, on=["id"], how="left")
"""
        plans = self.parser.parse_code(code)
        j = plans["result"]
        self.assertEqual(j.join_type, JoinType.LEFT)

    def test_join_on_condition(self):
        code = """
import pyspark.sql.functions as F
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.join(df2, on=F.col("id") == F.col("id"), how="inner")
"""
        plans = self.parser.parse_code(code)
        j = plans["result"]
        self.assertIsNotNone(j.condition)
        self.assertIsInstance(j.condition, BinaryOp)


class TestCaseWhenParsing(unittest.TestCase):
    def setUp(self):
        self.parser = make_parser()

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
        plans = self.parser.parse_code(code)
        sel = plans["result"]
        col = sel.columns[0]
        self.assertIsInstance(col, Alias)
        cw = col.expr
        self.assertIsInstance(cw, CaseWhen)
        self.assertEqual(len(cw.branches), 2)
        self.assertEqual(cw.default, Literal("C"))


class TestMiscParsing(unittest.TestCase):
    def setUp(self):
        self.parser = make_parser()

    def test_limit(self):
        code = """
df = spark.table("t")
result = df.limit(100)
"""
        plans = self.parser.parse_code(code)
        self.assertIsInstance(plans["result"], LimitPlan)
        self.assertEqual(plans["result"].n, 100)

    def test_distinct(self):
        code = """
df = spark.table("t")
result = df.distinct()
"""
        plans = self.parser.parse_code(code)
        self.assertIsInstance(plans["result"], DistinctPlan)

    def test_drop(self):
        code = """
df = spark.table("t")
result = df.drop("col1", "col2")
"""
        plans = self.parser.parse_code(code)
        self.assertIsInstance(plans["result"], DropPlan)
        self.assertEqual(plans["result"].columns, ("col1", "col2"))

    def test_with_column(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.withColumn("new_col", F.lit(1))
"""
        plans = self.parser.parse_code(code)
        wc = plans["result"]
        self.assertIsInstance(wc, WithColumnPlan)
        self.assertEqual(wc.column_name, "new_col")
        self.assertEqual(wc.expr, Literal(1))

    def test_rename(self):
        code = """
df = spark.table("t")
result = df.withColumnRenamed("old", "new")
"""
        plans = self.parser.parse_code(code)
        rn = plans["result"]
        self.assertIsInstance(rn, RenamePlan)
        self.assertEqual(rn.old_name, "old")
        self.assertEqual(rn.new_name, "new")

    def test_union(self):
        code = """
df1 = spark.table("t1")
df2 = spark.table("t2")
result = df1.union(df2)
"""
        plans = self.parser.parse_code(code)
        u = plans["result"]
        self.assertIsInstance(u, UnionPlan)
        self.assertEqual(len(u.sources), 2)

    def test_cast(self):
        code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.col("x").cast("string"))
"""
        plans = self.parser.parse_code(code)
        sel = plans["result"]
        cast = sel.columns[0]
        self.assertIsInstance(cast, Cast)
        self.assertEqual(cast.target_type, "string")

    def test_sample_pipeline(self):
        """サンプルパイプライン全体をエラーなく解析できること"""
        from pathlib import Path
        sample = Path("fixtures/pipelines/sample_basic.py").read_text()
        plans = self.parser.parse_code(sample)
        self.assertIn("result", plans)


if __name__ == "__main__":
    unittest.main()
