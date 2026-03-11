# PySpark → BigQuery SQL コンバーター

PySpark の DataFrame パイプラインを BigQuery 標準 SQL へ自動変換するツール。

## クイックスタート

```python
from converter import PySparkToBigQueryTransformer

transformer = PySparkToBigQueryTransformer()
result = transformer.convert_single("""
import pyspark.sql.functions as F
df = spark.table("raw.orders")
result = (
    df.filter(F.col("status") == "active")
      .groupBy("region")
      .agg(F.sum("amount").alias("total"))
)
""")
print(result.sql)
```

出力:
```sql
WITH
_cte_1 AS (
  SELECT *
  FROM `raw.orders`
  WHERE `status` = 'active'
)
SELECT
  `region`, SUM(`amount`) AS `total`
FROM _cte_1
GROUP BY `region`
```

## ローカル検証環境 (DuckDB)

```python
from validator import DuckDBRunner, TableSchema

runner = DuckDBRunner()
runner.register_table(TableSchema(
    name="raw.orders",
    columns=[("region","STRING"),("status","STRING"),("amount","FLOAT64")],
    data=[{"region":"East","status":"active","amount":100.0}],
))
result = runner.run(converted_sql)
print(result.rows)
```

## Docker 環境 (Spark + Jupyter)

```bash
docker compose up -d
# Jupyter: http://localhost:8888  (token: dev)
# Spark UI: http://localhost:4040
```

## テスト実行

```bash
python3 -m unittest discover -s tests -v
```
