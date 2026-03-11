"""
深いネスト・サブクエリを含むパイプライン

- 複数段階の集計 (groupBy → filter → groupBy)
- UNION
- dropDuplicates (subset)
- withColumnRenamed
- CTE が多段に展開されるケース
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

orders = spark.table("raw.orders")
customers = spark.table("raw.customers")
products = spark.table("raw.products")

# --- Step 1: 注文ごとの集計 ---
order_summary = (
    orders
    .filter(F.col("status").isin("active", "completed"))
    .groupBy("customer_id", "region")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("order_id").alias("order_count"),
        F.max("order_date").alias("last_order_date"),
    )
)

# --- Step 2: 高額顧客のフィルタ ---
high_value = (
    order_summary
    .filter(F.col("total_amount") > 100)
    .withColumnRenamed("total_amount", "revenue")
)

# --- Step 3: 低額顧客 ---
low_value = (
    order_summary
    .filter(F.col("total_amount") <= 100)
    .withColumnRenamed("total_amount", "revenue")
)

# --- Step 4: UNION ---
all_customers = high_value.union(low_value)

# --- Step 5: 顧客情報と JOIN ---
enriched = (
    all_customers
    .join(customers, on=["customer_id"], how="left")
    .select(
        "customer_id",
        "name",
        "region",
        "revenue",
        "order_count",
        "last_order_date",
        "tier",
    )
)

# --- Step 6: リージョンごとの二次集計 ---
region_summary = (
    enriched
    .groupBy("region")
    .agg(
        F.count("customer_id").alias("customer_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_revenue"),
        F.max("order_count").alias("max_orders"),
    )
    .orderBy(F.col("total_revenue").desc())
)

# --- Step 7: dropDuplicates + ランキング ---
w_rank = Window.partitionBy("region").orderBy(F.col("revenue").desc())
ranked = (
    enriched
    .dropDuplicates(["customer_id"])
    .withColumn("rank_in_region", F.row_number().over(w_rank))
    .filter(F.col("rank_in_region") <= 3)
    .select("customer_id", "name", "region", "revenue", "rank_in_region")
    .orderBy("region", "rank_in_region")
)
