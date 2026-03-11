# PySpark → BigQuery SQL コンバーター プロジェクト

## プロジェクト概要

既存の PySpark パイプラインを BigQuery 標準 SQL へ移行するためのコンバーターを開発する。
ローカル環境で Spark (Docker) と BigQuery 互換 DB (DuckDB) を立ち上げ、変換結果を双方で検証しながら移行を進める。

---

## ディレクトリ構成

```
pyspark-to-bq-converter/
├── CLAUDE.md                      # このファイル
├── README.md
├── docker-compose.yml             # Spark (bitnami/spark:3.5) + Jupyter 環境
├── pyproject.toml
│
├── converter/                     # コンバーター本体
│   ├── __init__.py                # PySparkToBigQueryTransformer をエクスポート
│   ├── ir.py                      # 中間表現ノード定義 (Expr / Plan)
│   ├── parser.py                  # PySpark AST パーサー (PySparkParser)
│   ├── transformer.py             # 統合エントリポイント (PySparkToBigQueryTransformer)
│   ├── emitter.py                 # IR → BigQuery SQL 文字列生成 (SQLEmitter)
│   └── rules/
│       ├── __init__.py
│       ├── functions.py           # 129 関数のマッピングレジストリ (@register デコレータ)
│       └── types.py               # PySpark DataType → BigQuery 型名変換
│
├── validator/                     # ローカル検証基盤
│   ├── __init__.py
│   ├── duckdb_runner.py           # DuckDB (BQ互換) で SQL 実行 / sqlite3 フォールバック
│   ├── comparator.py              # 実行結果の差分比較 (ResultComparator)
│   └── spark_runner.py            # PySpark で DataFrame 実行 (SparkRunner / InProcessSparkRunner)
│
├── fixtures/
│   ├── data/
│   │   ├── orders.csv             # テスト用注文データ (10行)
│   │   ├── customers.csv          # テスト用顧客データ (5行)
│   │   ├── products.csv           # テスト用商品データ (5行)
│   │   └── schemas.json           # テーブルスキーマ定義
│   └── pipelines/
│       ├── sample_basic.py        # サンプル PySpark パイプライン (filter/join/groupBy/window)
│       ├── sample_udf.py          # UDF 含むパイプライン (変換不可パターン検出テスト)
│       ├── sample_complex_window.py # 複雑 Window (rowsBetween/lag/lead/rank/累計)
│       └── sample_nested_select.py  # 深いネスト (多段集計/UNION/dropDuplicates/withColumnRenamed)
│
├── tests/
│   ├── unit/
│   │   ├── test_parser.py         # パーサー単体テスト 23件
│   │   └── test_transformer.py    # 変換・エミッター単体テスト 41件
│   └── integration/
│       ├── test_duckdb_validation.py  # DuckDB 統合テスト 35件
│       └── test_pipeline_validation.py # パイプライン統合テスト 15件
│
├── scripts/
│   ├── convert.py                 # 単一ファイル変換 CLI
│   └── batch_convert.py           # ディレクトリ一括変換 + レポート出力
```

---

## ローカル環境構成

### 使用技術

| 役割 | ツール | 理由 |
|------|--------|------|
| PySpark 実行 | Apache Spark (Docker: bitnami/spark:3.5) | 既存コードをそのまま検証 |
| BigQuery 互換 DB | **DuckDB** | BQ 標準 SQL 方言に最も近い、ローカル完結、高速 |
| フォールバック | sqlite3 (標準ライブラリ) | DuckDB 未インストール時の限定検証 |
| Jupyter | jupyter/pyspark-notebook:spark-3.5.0 | 対話的な検証・デバッグ |

### Docker 環境の起動

```bash
docker compose up -d
# Spark UI:  http://localhost:4040
# Spark Master UI: http://localhost:8080
# Jupyter:   http://localhost:8888  (token: dev)
```

### DuckDB を BQ 互換として使う理由

- `DATE_TRUNC`, `ARRAY_AGG`, `STRUCT`, `REGEXP_EXTRACT` など BQ 固有関数の多くをサポート
- `NULLS LAST / FIRST`, `EXCEPT (col)`, CTE など BQ の SQL 方言と高い互換性
- `PARSE_DATE / FORMAT_DATE` は strptime/strftime に自動変換
- 完全ローカル動作（課金・認証不要）

---

## コンバーター設計方針

### 変換フロー

```
PySpark スクリプト (.py)
        │
        ▼
  [1] parser.py          Python ast モジュールで AST 解析
        │                DataFrame API 呼び出しチェーンを IR (Plan ツリー) に変換
        ▼
  [2] emitter.py         Plan ツリーを再帰的に辿り BigQuery SQL を生成
        │                複雑なネストは自動で WITH 句 (CTE) に展開
        │                関数変換は rules/functions.py のレジストリに委譲
        ▼
  BigQuery SQL (WITH ... SELECT ...)
```

`transformer.py` は parser + emitter を束ねる統合エントリポイント。

### 中間表現 (IR) — converter/ir.py

**Expr ノード（式）:**

| クラス | 対応 PySpark | 備考 |
|--------|-------------|------|
| `ColRef` | `F.col("x")`, `"x"` | table_alias 付きも対応 |
| `Literal` | `F.lit(v)` | None/bool/str/int/float/list |
| `Alias` | `.alias("name")` | |
| `BinaryOp` | `a + b`, `a == b`, `a & b` | |
| `UnaryOp` | `~cond`, `-col` | |
| `FunctionCall` | `F.upper(col)` | name + args + kwargs |
| `CaseWhen` | `F.when(...).otherwise(...)` | branches tuple |
| `WindowExpr` | `.over(window_spec)` | partition / order / frame |
| `Cast` | `.cast("string")` | target_type は rules/types.py で変換 |
| `IsNull` / `IsNotNull` | `.isNull()` / `.isNotNull()` | |
| `InList` | `.isin([...])` | |
| `StarExpr` | `"*"` | |
| `OrderSpec` | `.asc()` / `.desc()` | ascending + nulls_last |
| `WindowFrame` | `.rowsBetween(...)` | ROWS/RANGE BETWEEN |

**Plan ノード（論理プラン）:**

| クラス | 対応 PySpark | 生成 SQL |
|--------|-------------|---------|
| `SourceTable` | `spark.table("t")` / `spark.read.parquet(...)` | `FROM \`t\`` |
| `SelectPlan` | `.select(...)` | `SELECT cols FROM ...` |
| `FilterPlan` | `.filter(...)` / `.where(...)` | `WHERE cond` |
| `GroupByPlan` | `.groupBy(...).agg(...)` | `GROUP BY ... + 集計` |
| `JoinPlan` | `.join(other, on=..., how=...)` | `INNER/LEFT/RIGHT/FULL OUTER JOIN` |
| `OrderByPlan` | `.orderBy(...)` / `.sort(...)` | `ORDER BY` |
| `LimitPlan` | `.limit(n)` | `LIMIT n` |
| `DistinctPlan` | `.distinct()` / `.dropDuplicates(...)` | `SELECT DISTINCT *` / ROW_NUMBER() 方式 |
| `UnionPlan` | `.union(...)` / `.unionByName(...)` | `UNION ALL` / `UNION DISTINCT` |
| `WithColumnPlan` | `.withColumn(name, expr)` | `SELECT *, expr AS name` |
| `DropPlan` | `.drop(...)` | `SELECT * EXCEPT(col)` |
| `RenamePlan` | `.withColumnRenamed(old, new)` | `SELECT * EXCEPT(old), old AS new` |
| `SubqueryPlan` | `.alias("name")` | `(subquery) AS name` |

### CTE 自動展開ロジック

emitter は Plan ツリーを再帰的に処理し、SourceTable 以外のノードを FROM 句で使う際は自動で CTE (_cte_1, _cte_2, ...) に変換する。最終 SQL は WITH 句で全 CTE を宣言してから SELECT する形式になる。

```sql
WITH
_cte_1 AS (...),
_cte_2 AS (...),
...
SELECT * FROM _cte_N ORDER BY ...
```

---

## 関数マッピング — converter/rules/functions.py

`@register` デコレータでレジストリ (`_REGISTRY: dict[str, FuncHandler]`) に登録。
ハンドラ型: `(args: list[str], kwargs: dict[str, str]) -> str`

### NULL 系
| PySpark | BigQuery |
|---------|---------|
| `F.coalesce(a, b)` | `COALESCE(a, b)` |
| `F.nullif(a, b)` | `NULLIF(a, b)` |
| `F.nvl(a, b)` | `IFNULL(a, b)` |

### 文字列系 (主要)
| PySpark | BigQuery |
|---------|---------|
| `F.upper(c)` | `UPPER(c)` |
| `F.lower(c)` | `LOWER(c)` |
| `F.trim(c)` | `TRIM(c)` |
| `F.substr(c, pos, len)` | `SUBSTR(c, pos, len)` |
| `F.concat(a, b)` | `CONCAT(a, b)` |
| `F.concat_ws(sep, c1, c2)` | `ARRAY_TO_STRING([c1,c2], sep)` |
| `F.split(c, pat)` | `SPLIT(c, pat)` |
| `F.regexp_replace(c, p, r)` | `REGEXP_REPLACE(c, p, r)` |
| `F.regexp_extract(c, p)` | `REGEXP_EXTRACT(c, p)` |
| `F.instr(c, s)` | `STRPOS(c, s)` |
| `F.lpad(c, n, p)` | `LPAD(c, n, p)` |
| `F.initcap(c)` | `INITCAP(c)` |
| `F.format_string(fmt, ...)` | `FORMAT(fmt, ...)` |

### 数値系 (主要)
| PySpark | BigQuery |
|---------|---------|
| `F.abs(c)` | `ABS(c)` |
| `F.ceil(c)` / `F.floor(c)` | `CEIL(c)` / `FLOOR(c)` |
| `F.round(c, n)` | `ROUND(c, n)` |
| `F.pow(a, b)` | `POWER(a, b)` |
| `F.log(base, c)` | `LOG(c, base)` |
| `F.log(c)` (1引数) | `LN(c)` |
| `F.rand()` | `RAND()` |
| `F.greatest(...)` | `GREATEST(...)` |
| `F.least(...)` | `LEAST(...)` |
| `F.pmod(a, b)` | `MOD(a, b)` |

### 日付・時刻系 (主要)
| PySpark | BigQuery |
|---------|---------|
| `F.current_date()` | `CURRENT_DATE()` |
| `F.current_timestamp()` | `CURRENT_TIMESTAMP()` |
| `F.to_date(c, fmt)` | `PARSE_DATE(fmt, c)` |
| `F.to_timestamp(c, fmt)` | `PARSE_TIMESTAMP(fmt, c)` |
| `F.date_format(c, fmt)` | `FORMAT_DATE(fmt, c)` |
| `F.date_add(c, n)` | `DATE_ADD(c, INTERVAL n DAY)` |
| `F.date_sub(c, n)` | `DATE_SUB(c, INTERVAL n DAY)` |
| `F.datediff(end, start)` | `DATE_DIFF(end, start, DAY)` |
| `F.months_between(a, b)` | `DATE_DIFF(a, b, MONTH)` |
| `F.add_months(c, n)` | `DATE_ADD(c, INTERVAL n MONTH)` |
| `F.trunc(c, "MM")` | `DATE_TRUNC(c, MONTH)` |
| `F.year(c)` | `EXTRACT(YEAR FROM c)` |
| `F.month(c)` | `EXTRACT(MONTH FROM c)` |
| `F.dayofmonth(c)` | `EXTRACT(DAY FROM c)` |
| `F.hour(c)` / `F.minute(c)` / `F.second(c)` | `EXTRACT(HOUR/MINUTE/SECOND FROM c)` |
| `F.unix_timestamp(c)` | `UNIX_SECONDS(TIMESTAMP(c))` |
| `F.from_unixtime(c)` | `FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(CAST(c AS INT64)))` |

### 集計系
| PySpark | BigQuery |
|---------|---------|
| `F.count("*")` | `COUNT(*)` |
| `F.countDistinct(c)` | `COUNT(DISTINCT c)` |
| `F.sum(c)` / `F.avg(c)` | `SUM(c)` / `AVG(c)` |
| `F.min(c)` / `F.max(c)` | `MIN(c)` / `MAX(c)` |
| `F.first(c)` | `ANY_VALUE(c)` |
| `F.collect_list(c)` | `ARRAY_AGG(c IGNORE NULLS)` |
| `F.collect_set(c)` | `ARRAY_AGG(DISTINCT c IGNORE NULLS)` |
| `F.stddev(c)` | `STDDEV_SAMP(c)` |
| `F.variance(c)` | `VAR_SAMP(c)` |
| `F.approxCountDistinct(c)` | `APPROX_COUNT_DISTINCT(c)` |

### Window 系
| PySpark | BigQuery |
|---------|---------|
| `F.row_number()` | `ROW_NUMBER()` |
| `F.rank()` | `RANK()` |
| `F.dense_rank()` | `DENSE_RANK()` |
| `F.lag(c, n, default)` | `LAG(c, n, default)` |
| `F.lead(c, n, default)` | `LEAD(c, n, default)` |
| `F.first_value(c)` | `FIRST_VALUE(c)` |
| `F.last_value(c)` | `LAST_VALUE(c)` |
| `F.ntile(n)` | `NTILE(n)` |

### 配列・JSON 系 (主要)
| PySpark | BigQuery |
|---------|---------|
| `F.array(a, b)` | `[a, b]` |
| `F.array_contains(arr, v)` | `v IN UNNEST(arr)` |
| `F.array_join(arr, sep)` | `ARRAY_TO_STRING(arr, sep)` |
| `F.size(arr)` | `ARRAY_LENGTH(arr)` |
| `F.collect_list(c)` | `ARRAY_AGG(c IGNORE NULLS)` |
| `F.get_json_object(c, path)` | `JSON_EXTRACT(c, path)` |
| `F.to_json(c)` | `TO_JSON_STRING(c)` |
| `F.struct(...)` | `STRUCT(...)` |

### Hash 系
| PySpark | BigQuery |
|---------|---------|
| `F.md5(c)` | `TO_HEX(MD5(c))` |
| `F.sha1(c)` | `TO_HEX(SHA1(c))` |
| `F.hash(c)` | `FARM_FINGERPRINT(c)` |

---

## 型マッピング — converter/rules/types.py

`convert_type(spark_type: str) -> str` で変換。

| PySpark | BigQuery |
|---------|---------|
| IntegerType / int / long | INT64 |
| FloatType / DoubleType / double | FLOAT64 |
| StringType / string / varchar | STRING |
| BooleanType / boolean | BOOL |
| DateType / date | DATE |
| TimestampType / timestamp | TIMESTAMP |
| BinaryType / binary | BYTES |
| DecimalType / decimal | NUMERIC |
| decimal(18, 3) | NUMERIC(18, 3) |
| ArrayType / array | ARRAY<...> |
| MapType / map | JSON |
| StructType / struct | STRUCT<...> |

---

## 検証基盤 — validator/

### DuckDB Runner (duckdb_runner.py)

```python
from validator import DuckDBRunner, TableSchema

runner = DuckDBRunner()
runner.register_table(TableSchema(
    name="raw.orders",
    columns=[("order_id","INT64"), ("status","STRING"), ("amount","FLOAT64")],
    data=[{"order_id": 1, "status": "active", "amount": 100.0}],
))
# CSV からの読み込みも可能
runner.register_table(TableSchema.from_csv("raw.orders", "fixtures/data/orders.csv"))

result = runner.run(converted_sql)
print(result.rows)      # list[tuple]
print(result.to_dicts()) # list[dict]
```

**BQ → DuckDB 自動変換 (_adapt_sql):**
- バッククォート → ダブルクォート
- `raw.orders` → `raw_orders` (safe name)
- `CURRENT_DATE()` → `CURRENT_DATE`
- `PARSE_DATE(fmt, c)` → `strptime(c, fmt)`
- `FARM_FINGERPRINT(c)` → `hash(c)`
- `APPROX_COUNT_DISTINCT` → `approx_count_distinct`

**フォールバック (sqlite3):** duckdb 未インストール時は sqlite3 で限定実行。Window 関数・EXCEPT・型付き集計を使うテストは自動 skip。

### Comparator (comparator.py)

```python
from validator import ResultComparator, CompareConfig

cmp = ResultComparator(CompareConfig(
    ignore_row_order=True,       # 行順序無視 (デフォルト True)
    ignore_column_order=True,    # カラム順序無視 (デフォルト True)
    float_tolerance=1e-9,        # 浮動小数点許容誤差
    null_vs_empty_string="warn", # "" を NULL として扱い警告
))
result = cmp.compare(spark_result, duckdb_result)
result.assert_equal()    # 不一致なら AssertionError
print(result.summary())  # テキストレポート
```

### Spark Runner (spark_runner.py)

```python
# Docker 環境
from validator import SparkRunner, SparkConfig
runner = SparkRunner(SparkConfig(master="spark://localhost:7077", use_local=False))

# インプロセス (pyspark インストール済みの場合)
from validator.spark_runner import InProcessSparkRunner
runner = InProcessSparkRunner()
result = runner.run(pyspark_code, var_name="result", tables={"raw.orders": [...]})
```

---

## テスト状況

```
tests/unit/test_parser.py          23件  全件 PASS
tests/unit/test_transformer.py     41件  全件 PASS
tests/integration/test_duckdb_validation.py
                                   35件  PASS (6件 sqlite skip / duckdb では全件 PASS)
tests/integration/test_pipeline_validation.py
                                   15件  PASS (4件 sqlite skip / duckdb では全件 PASS)
─────────────────────────────────────────
合計                               114件  PASS (14 skipped)
```

テスト実行:
```bash
python3 -m unittest discover -s tests -v
```

---

## 既知の変換困難パターン（要手動確認）

| パターン | 理由 | 対処 |
|---------|------|------|
| `rdd.map()` / `rdd.flatMap()` | RDD API は SQL 非対応 | UDF 化 or 手動書き換え |
| `pandas_udf` / `udf(...)` | BQ は Python UDF 非対応 | BQ Remote Function または SQL に書き直し |
| `F.explode(arr)` | FROM 句の UNNEST が必要 | `/* WARNING */` コメント付きで出力 |
| `F.posexplode(arr)` | WITH OFFSET 構文が必要 | 同上 |
| `LEFT SEMI JOIN` / `LEFT ANTI JOIN` | BQ は非対応 | EXISTS / NOT EXISTS サブクエリに書き換え (警告出力) |
| `dropDuplicates(subset)` | ROW_NUMBER() OVER (PARTITION BY) で代替 | 警告出力 + 自動変換 |
| `withColumnRenamed` | SELECT * EXCEPT + alias で代替 | 警告出力 + 自動変換 |
| `spark.read.parquet(path)` | BQ はロード済みテーブルが必要 | 警告出力 + TODO コメント |
| `spark.read.jdbc(...)` | BQ は JDBC 非対応 | 手動で Federated Query または Transfer に置換 |
| `df.cache()` / `df.persist()` | BQ はキャッシュ自動 | 削除してよい |
| `broadcast(df)` | BQ オプティマイザが自動判断 | 削除してよい (変換時にそのまま式を返す) |
| `monotonically_increasing_id()` | BQ 非対応 | `ROW_NUMBER() OVER ()` に変換 + 警告 |
| 複雑な動的スキーマ (`schema_of_json` 等) | BQ は静的スキーマ必須 | 事前スキーマ定義が必要 |
| `F.factorial(c)` | BQ 未対応 | `/* WARNING */` + NULL 返却 |
| map 型 (`F.map_keys` 等) | BQ は JSON で代替 | `/* WARNING */` 付きで出力 |

変換時に `/* WARNING: ... */` コメントが SQL 中に含まれる場合、`ConversionResult.unsupported_patterns` にも記録される。

---

## 作業進め方（フェーズ）

### Phase 1: コンバーター MVP ✅ 完了
- [x] `ir.py`: IR ノード定義 (Expr 14種 / Plan 14種)
- [x] `parser.py`: DataFrame API チェーン全パターンの AST 解析
- [x] `emitter.py`: CTE 自動展開 + SQL 生成
- [x] `transformer.py`: 統合エントリポイント
- [x] `rules/functions.py`: 129 関数マッピング
- [x] `rules/types.py`: 型マッピング
- [x] 単体テスト 64件 全件 PASS

### Phase 2: 検証基盤 ✅ 完了
- [x] `docker-compose.yml`: Spark + Jupyter 環境
- [x] `validator/duckdb_runner.py`: DuckDB runner + sqlite3 フォールバック
- [x] `validator/comparator.py`: 結果差分比較
- [x] `validator/spark_runner.py`: Spark runner (Docker / InProcess)
- [x] `fixtures/data/`: CSV サンプルデータ 3テーブル
- [x] 統合テスト 35件 全件 PASS

### Phase 3: CLI & 実パイプライン移行 ✅ 完了
- [x] `scripts/convert.py`: 単一ファイル変換 CLI (--var / --out / --validate)
- [x] `scripts/batch_convert.py`: ディレクトリ一括変換 + JSON レポート出力 (--out / --report / --recursive)
- [x] サンプルパイプライン 3本追加 (UDF / 複雑 Window / 深いネスト)
- [x] 自動変換率計測: 75% (4パイプライン中 3本が警告なし変換成功)
- [x] パイプライン統合テスト 15件 全件 PASS

---

## API クイックリファレンス

```python
from converter import PySparkToBigQueryTransformer

t = PySparkToBigQueryTransformer()

# 文字列から変換 → 全変数を返す
results: dict[str, ConversionResult] = t.convert_code(source_code)

# ファイルから変換
results = t.convert_file("pipeline.py")

# 最後の変数だけ変換 (スクリプト1本 → SQL 1本のショートカット)
result: ConversionResult = t.convert_single(source_code)

print(result.sql)                  # 生成 SQL
print(result.warnings)             # 変換警告リスト
print(result.unsupported_patterns) # /* WARNING: */ を含む行のリスト
```

---

## Claude への作業指示

### 新しい関数マッピングを追加するとき

1. `converter/rules/functions.py` に `@register("func_name")` デコレータ付き関数を追加
2. `tests/unit/test_transformer.py` に対応するテストを追加
3. 本ファイルの関数マッピング表を更新

### 新しい Plan ノードを追加するとき

1. `converter/ir.py` に `@dataclass(frozen=True)` でノードを追加
2. `converter/parser.py` の `_emit_plan` に対応するケースを追加
3. `converter/emitter.py` の `_emit_plan` に対応する emit メソッドを追加
4. 単体テストを追加

### 統合テストを追加するとき

1. `fixtures/pipelines/` に対象 PySpark スクリプトを追加
2. `fixtures/data/` に対応する CSV サンプルデータを追加 (なければ)
3. `fixtures/data/schemas.json` にスキーマを追加
4. `tests/integration/test_duckdb_validation.py` にテストケースを追加

### 変換できないパターンを発見したとき

1. 本ファイルの「既知の変換困難パターン」テーブルに追記
2. `converter/emitter.py` or `rules/functions.py` で `/* WARNING: ... */` コメントを出力
3. `ConversionResult.unsupported_patterns` に自動収集される

---

## コーディング規約

- 型ヒントを必ず付ける (`from __future__ import annotations`)
- IR ノードは `@dataclass(frozen=True)` で不変に
- 変換ルール追加時は必ず対応する単体テストを追加
- 変換できないパターンは `/* WARNING: ... */` コメントで SQL 中に記録
- テスト実行は `python3 -m unittest discover -s tests -v`
