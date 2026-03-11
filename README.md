# PySpark → BigQuery SQL コンバーター

PySpark の DataFrame パイプラインを BigQuery 標準 SQL へ自動変換するツール。

## 必要環境

- Python 3.11+
- Docker (Spark + Jupyter 環境を使う場合)
- DuckDB (`pip install duckdb`、変換後 SQL の検証に使用)

## セットアップ

```bash
# 依存パッケージ (開発用)
pip install -e ".[dev]"

# DuckDB (変換後 SQL のローカル検証用)
pip install duckdb
```

## 基本的な使い方

### 一括変換 (base_code/ → converted_code/)

`base_code/` に PySpark の `.py` ファイルを置いて実行:

```bash
python3 scripts/batch_convert.py
```

`converted_code/` に変換後の `.sql` ファイルが生成される。

### 単一ファイル変換

```bash
# stdout に出力
python3 scripts/convert.py base_code/sample_basic.py

# ファイルに出力
python3 scripts/convert.py base_code/sample_basic.py --out converted_code/sample_basic.sql

# 特定の変数だけ変換
python3 scripts/convert.py base_code/sample_basic.py --var result

# DuckDB で構文チェック
python3 scripts/convert.py base_code/sample_basic.py --validate
```

### Python API

```python
from converter import PySparkToBigQueryTransformer

t = PySparkToBigQueryTransformer()

# 文字列から変換
results = t.convert_code(source_code)       # dict[str, ConversionResult]

# ファイルから変換
results = t.convert_file("pipeline.py")

# 最後の変数だけ変換
result = t.convert_single(source_code)      # ConversionResult

print(result.sql)                  # 生成 SQL
print(result.warnings)             # 変換警告リスト
print(result.unsupported_patterns) # /* WARNING: */ を含む行のリスト
```

## テスト

```bash
# 全テスト実行
python3 -m unittest discover -s tests -v

# 単体テストのみ
python3 -m unittest discover -s tests/unit -v

# 統合テストのみ
python3 -m unittest discover -s tests/integration -v
```

DuckDB がインストールされていない場合、一部の統合テストは sqlite3 フォールバックで実行され、Window 関数や EXCEPT 構文を使うテストは自動 skip される。

## Docker 環境 (Spark + Jupyter)

### 起動

```bash
docker compose up -d
```

| サービス | URL | 備考 |
|---------|-----|------|
| Spark Master UI | http://localhost:8080 | クラスタ状態・Worker 確認 |
| Jupyter Lab | http://localhost:8888 | token: `dev` |

※ `:4040` (Spark Application UI) は Spark ジョブ実行中のみアクセス可能。

### Jupyter で Spark を使う

Jupyter 上では `local[*]` モードで実行する:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

orders = spark.read.csv("/home/jovyan/fixtures/data/orders.csv", header=True, inferSchema=True)
orders.show()
```

サンプルノートブック: `work/spark/sample.ipynb`

### 停止

```bash
docker compose down
```

## DuckDB で変換後 SQL を検証する

```python
from validator import DuckDBRunner, TableSchema

runner = DuckDBRunner()

# CSV からテーブル登録
runner.register_table(TableSchema.from_csv("raw.orders", "fixtures/data/orders.csv"))
runner.register_table(TableSchema.from_csv("raw.customers", "fixtures/data/customers.csv"))

# 変換後 SQL を実行
result = runner.run(converted_sql)
print(result.rows)       # list[tuple]
print(result.to_dicts()) # list[dict]
```

DuckDB Runner は BigQuery SQL を自動で DuckDB 方言に変換する（バッククォート→ダブルクォート、PARSE_DATE→strptime 等）。

## Spark と DuckDB の結果を比較する

```python
from validator import ResultComparator, CompareConfig

cmp = ResultComparator(CompareConfig(
    ignore_row_order=True,
    ignore_column_order=True,
    float_tolerance=1e-9,
))
result = cmp.compare(spark_result, duckdb_result)
result.assert_equal()
print(result.summary())
```
