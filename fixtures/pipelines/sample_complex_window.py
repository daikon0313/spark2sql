"""
複雑な Window 関数を含むパイプライン

- rowsBetween / rangeBetween
- 複数の Window 関数 (row_number, rank, lag, lead)
- 累計・移動平均
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

orders = spark.table("raw.orders")
customers = spark.table("raw.customers")

# --- JOIN ---
joined = orders.join(customers, on=["customer_id"], how="left")

# --- Window 定義 ---
w_region = Window.partitionBy("region").orderBy(F.col("order_date").asc())
w_customer = Window.partitionBy("customer_id").orderBy(F.col("order_date").asc())
w_running = (
    Window.partitionBy("region")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# --- 複数の Window 関数適用 ---
windowed = (
    joined
    .withColumn("region_row_num", F.row_number().over(w_region))
    .withColumn("region_rank", F.rank().over(w_region))
    .withColumn("prev_amount", F.lag(F.col("amount"), 1).over(w_customer))
    .withColumn("next_amount", F.lead(F.col("amount"), 1).over(w_customer))
    .withColumn("running_total", F.sum(F.col("amount")).over(w_running))
)

# --- 最終 SELECT ---
result = (
    windowed
    .select(
        "order_id",
        "customer_id",
        "region",
        "amount",
        "order_date",
        "region_row_num",
        "region_rank",
        "prev_amount",
        "next_amount",
        "running_total",
    )
    .orderBy("region", "order_date")
)
