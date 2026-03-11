"""サンプル PySpark パイプライン (基本パターン)"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sample").getOrCreate()

# テーブル読み込み
orders = spark.table("raw.orders")
customers = spark.table("raw.customers")

# フィルタ
active_orders = orders.filter(F.col("status") == "active")

# JOIN
joined = active_orders.join(customers, on=["customer_id"], how="left")

# 集計
summary = (
    joined
    .groupBy("customer_id", "region")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("order_id").alias("order_count"),
        F.max("order_date").alias("last_order_date"),
    )
)

# Window 関数
window_spec = Window.partitionBy("region").orderBy(F.col("total_amount").desc())
ranked = summary.withColumn("rank", F.row_number().over(window_spec))

# 最終 SELECT
result = (
    ranked
    .filter(F.col("rank") <= 10)
    .select(
        "customer_id",
        "region",
        "total_amount",
        "order_count",
        "last_order_date",
        "rank",
    )
    .orderBy("region", "rank")
)
