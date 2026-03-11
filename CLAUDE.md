# PySpark → BigQuery SQL コンバーター

## プロジェクト概要

PySpark の DataFrame パイプライン (.py) を BigQuery 標準 SQL へ自動変換するツール。
`base_code/` に PySpark スクリプトを置き、コンバーターを実行すると `converted_code/` に SQL が生成される。
ローカルでは DuckDB を BigQuery 互換 DB として使い、変換結果を検証する。

## ディレクトリ構成

```
spark2sql/
├── CLAUDE.md                        # このファイル（プロジェクト構造・設計の説明）
├── README.md                        # 環境構築・使い方
├── docker-compose.yml               # Spark (apache/spark:3.5.0) + Jupyter 環境
├── pyproject.toml
│
├── base_code/                       # 変換元: PySpark スクリプトを置く
├── converted_code/                  # 変換先: 生成された BigQuery SQL（.gitignore 対象）
│
├── converter/                       # コンバーター本体
│   ├── __init__.py                  #   PySparkToBigQueryTransformer をエクスポート
│   ├── ir.py                        #   中間表現 (IR) ノード定義 (Expr / Plan)
│   ├── parser.py                    #   PySpark AST パーサー (PySparkParser)
│   ├── transformer.py               #   統合エントリポイント (parser + emitter)
│   ├── emitter.py                   #   IR → BigQuery SQL 文字列生成 (SQLEmitter)
│   └── rules/
│       ├── __init__.py
│       ├── functions.py             #   129 関数マッピング (@register デコレータ)
│       ├── types.py                 #   PySpark DataType → BigQuery 型変換
│       ├── join.py                  #   JOIN 種別変換
│       └── window.py                #   Window 関数変換
│
├── validator/                       # ローカル検証基盤
│   ├── __init__.py
│   ├── duckdb_runner.py             #   DuckDB で変換後 SQL を実行 (sqlite3 フォールバック)
│   ├── comparator.py                #   Spark結果 vs DuckDB結果 の差分比較
│   └── spark_runner.py              #   PySpark で DataFrame 実行
│
├── fixtures/                        # テスト用データ・パイプライン
│   ├── data/
│   │   ├── orders.csv               #   注文データ (10行)
│   │   ├── customers.csv            #   顧客データ (5行)
│   │   ├── products.csv             #   商品データ (5行)
│   │   └── schemas.json             #   テーブルスキーマ定義
│   └── pipelines/                   #   テスト用 PySpark サンプル
│       ├── sample_basic.py
│       ├── sample_udf.py
│       ├── sample_complex_window.py
│       └── sample_nested_select.py
│
├── tests/
│   ├── unit/
│   │   ├── test_parser.py           #   パーサー単体テスト
│   │   └── test_transformer.py      #   変換・エミッター単体テスト
│   └── integration/
│       ├── test_duckdb_validation.py #   DuckDB 統合テスト
│       └── test_pipeline_validation.py # パイプライン統合テスト
│
├── scripts/
│   ├── convert.py                   #   単一ファイル変換 CLI
│   └── batch_convert.py             #   一括変換 CLI (デフォルト: base_code/ → converted_code/)
│
└── work/
    └── spark/
        └── sample.ipynb             #   Jupyter での Spark 動作確認用ノートブック
```

## 変換フロー

```
PySpark スクリプト (.py)
        │
        ▼
  [1] parser.py        Python ast モジュールで AST 解析
        │              DataFrame API チェーンを IR (Plan ツリー) に変換
        ▼
  [2] emitter.py       Plan ツリーを再帰的に辿り BigQuery SQL を生成
        │              複雑なネストは WITH 句 (CTE) に自動展開
        │              関数変換は rules/functions.py のレジストリに委譲
        ▼
  BigQuery SQL (WITH ... SELECT ...)
```

`transformer.py` が parser + emitter を束ねる統合エントリポイント。

## 中間表現 (IR) — converter/ir.py

### Expr ノード（式）

| クラス | 対応 PySpark | 備考 |
|--------|-------------|------|
| `ColRef` | `F.col("x")`, `"x"` | table_alias 付きも対応 |
| `Literal` | `F.lit(v)` | None/bool/str/int/float/list |
| `Alias` | `.alias("name")` | |
| `BinaryOp` | `a + b`, `a == b`, `a & b` | |
| `UnaryOp` | `~cond`, `-col` | |
| `FunctionCall` | `F.upper(col)` | name + args + kwargs |
| `CaseWhen` | `F.when(...).otherwise(...)` | |
| `WindowExpr` | `.over(window_spec)` | partition / order / frame |
| `Cast` | `.cast("string")` | rules/types.py で型変換 |
| `IsNull` / `IsNotNull` | `.isNull()` / `.isNotNull()` | |
| `InList` | `.isin([...])` | |
| `StarExpr` | `"*"` | |
| `OrderSpec` | `.asc()` / `.desc()` | |
| `WindowFrame` | `.rowsBetween(...)` | ROWS/RANGE BETWEEN |

### Plan ノード（論理プラン）

| クラス | 対応 PySpark | 生成 SQL |
|--------|-------------|---------|
| `SourceTable` | `spark.table("t")` / `spark.read.*` | `FROM \`t\`` |
| `SelectPlan` | `.select(...)` | `SELECT cols FROM ...` |
| `FilterPlan` | `.filter(...)` / `.where(...)` | `WHERE cond` |
| `GroupByPlan` | `.groupBy(...).agg(...)` | `GROUP BY ...` |
| `JoinPlan` | `.join(other, on=..., how=...)` | `JOIN` |
| `OrderByPlan` | `.orderBy(...)` / `.sort(...)` | `ORDER BY` |
| `LimitPlan` | `.limit(n)` | `LIMIT n` |
| `DistinctPlan` | `.distinct()` / `.dropDuplicates(...)` | `DISTINCT` / ROW_NUMBER() |
| `UnionPlan` | `.union(...)` / `.unionByName(...)` | `UNION ALL` / `UNION DISTINCT` |
| `WithColumnPlan` | `.withColumn(name, expr)` | `SELECT *, expr AS name` |
| `DropPlan` | `.drop(...)` | `SELECT * EXCEPT(col)` |
| `RenamePlan` | `.withColumnRenamed(old, new)` | `SELECT * EXCEPT(old), old AS new` |
| `SubqueryPlan` | `.alias("name")` | `(subquery) AS name` |

## 関数マッピング — converter/rules/functions.py

`@register` デコレータでレジストリに登録（129関数）。主要なカテゴリ:

- **NULL 系**: coalesce, nullif, nvl
- **文字列系**: upper, lower, trim, substr, concat, concat_ws, split, regexp_replace, regexp_extract, lpad, initcap 等
- **数値系**: abs, ceil, floor, round, pow, log, rand, greatest, least 等
- **日付・時刻系**: current_date, to_date, to_timestamp, date_format, date_add, date_sub, datediff, year, month 等
- **集計系**: count, countDistinct, sum, avg, min, max, first, collect_list, collect_set, stddev, variance 等
- **Window 系**: row_number, rank, dense_rank, lag, lead, first_value, last_value, ntile
- **配列・JSON 系**: array, array_contains, size, get_json_object, to_json, struct 等
- **Hash 系**: md5, sha1, hash

## 型マッピング — converter/rules/types.py

| PySpark | BigQuery |
|---------|---------|
| IntegerType / int / long | INT64 |
| FloatType / DoubleType | FLOAT64 |
| StringType / string | STRING |
| BooleanType | BOOL |
| DateType | DATE |
| TimestampType | TIMESTAMP |
| DecimalType / decimal(p,s) | NUMERIC / NUMERIC(p,s) |
| ArrayType | ARRAY<...> |
| MapType | JSON |
| StructType | STRUCT<...> |

## 既知の変換困難パターン

| パターン | 理由 | 対処 |
|---------|------|------|
| `rdd.map()` / `rdd.flatMap()` | RDD API は SQL 非対応 | 手動書き換え |
| `pandas_udf` / `udf(...)` | BQ は Python UDF 非対応 | BQ Remote Function or SQL 書き直し |
| `F.explode(arr)` / `F.posexplode(arr)` | UNNEST 構文が必要 | `/* WARNING */` 付きで出力 |
| `LEFT SEMI/ANTI JOIN` | BQ 非対応 | EXISTS / NOT EXISTS に変換 + 警告 |
| `spark.read.parquet(path)` / `spark.read.jdbc(...)` | BQ はロード済みテーブル前提 | 警告 + TODO コメント |
| `df.cache()` / `broadcast(df)` | BQ では不要 | 自動削除 |
| `monotonically_increasing_id()` | BQ 非対応 | `ROW_NUMBER() OVER ()` + 警告 |
| `F.factorial(c)` / map 型関数 | BQ 未対応 | `/* WARNING */` 付きで出力 |

変換時に `/* WARNING: ... */` が SQL に含まれる場合、`ConversionResult.unsupported_patterns` にも自動収集される。

## コーディング規約

- 型ヒントを必ず付ける (`from __future__ import annotations`)
- IR ノードは `@dataclass(frozen=True)` で不変に
- 変換ルール追加時は必ず対応する単体テストを追加
- 変換できないパターンは `/* WARNING: ... */` コメントで SQL 中に記録

## Claude への作業指示

### 新しい関数マッピングを追加するとき

1. `converter/rules/functions.py` に `@register("func_name")` で追加
2. `tests/unit/test_transformer.py` にテスト追加
3. CLAUDE.md の関数マッピング説明を必要に応じて更新

### 新しい Plan ノードを追加するとき

1. `converter/ir.py` に `@dataclass(frozen=True)` でノード追加
2. `converter/parser.py` に対応パース処理を追加
3. `converter/emitter.py` に対応 emit メソッドを追加
4. 単体テスト追加

### プロジェクトに変更を加えたとき

**CLAUDE.md は常に最新の状態を保つこと。** 以下の変更時に必ず見直す:
- ディレクトリ構成の変更
- 新しいモジュール・ファイルの追加
- IR ノードや関数マッピングの追加・変更
- 変換困難パターンの発見
- コーディング規約の変更
