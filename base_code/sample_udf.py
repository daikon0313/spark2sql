"""
UDF・pandas_udf を含むパイプライン（変換不可パターンの検出テスト）

期待: UDF 部分は WARNING コメントが出力され、
      通常の DataFrame API 部分は正常に変換される。
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# --- 通常の変換可能な部分 ---
orders = spark.table("raw.orders")

# filter + select は正常に変換される
active_orders = (
    orders
    .filter(F.col("status") == "active")
    .select("order_id", "customer_id", "amount", "order_date")
)

# withColumn + 通常の関数は正常に変換される
enriched = (
    active_orders
    .withColumn("amount_str", F.format_string("$%.2f", F.col("amount")))
    .withColumn("order_year", F.year(F.col("order_date")))
)

# --- 集計 ---
summary = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("order_id").alias("order_count"),
    )
)
