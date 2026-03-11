# Claude Code 引き継ぎドキュメント

> このドキュメントは Claude Code でこのリポジトリの作業を引き継ぐための情報をまとめたものです。
> プロジェクト全体の設計は `CLAUDE.md` を参照してください。

---

## セッション概要

このプロジェクトは **claude.ai (Claude Sonnet 4.6)** との対話セッションで以下を実装・完成させました。

| フェーズ | 内容 | 状態 |
|---------|------|------|
| Phase 1 | コンバーター MVP (parser / emitter / transformer / rules) | ✅ 完了 |
| Phase 2 | 検証基盤 (docker-compose / duckdb_runner / comparator / spark_runner) | ✅ 完了 |
| Phase 3 | CLI スクリプト / 実パイプライン移行 | ✅ 完了 |

---

## すぐに動作確認する手順

```bash
# 1. テスト全件実行 (99件 PASS / 6件 sqlite skip が正常)
python3 -m unittest discover -s tests -v

# 2. サンプルパイプラインを変換してみる
python3 - << 'EOF'
from converter import PySparkToBigQueryTransformer
t = PySparkToBigQueryTransformer()
r = t.convert_file("fixtures/pipelines/sample_basic.py", target_vars=["result"])
print(r["result"].sql)
EOF

# 3. DuckDB でサンプル SQL を実際に実行してみる (duckdb インストール済みの場合)
pip install duckdb
python3 - << 'EOF'
from converter import PySparkToBigQueryTransformer
from validator import DuckDBRunner, TableSchema
import csv, json
from pathlib import Path

t = PySparkToBigQueryTransformer()
r = t.convert_file("fixtures/pipelines/sample_basic.py", target_vars=["result"])
sql = r["result"].sql

def load(name, file):
    rows = list(csv.DictReader(open(f"fixtures/data/{file}")))
    schemas = json.load(open("fixtures/data/schemas.json"))
    cols = [(c["name"], c["type"]) for c in schemas[name]["columns"]]
    return TableSchema(name=name, columns=cols, data=rows)

runner = DuckDBRunner()
runner.register_table(load("raw.orders", "orders.csv"))
runner.register_table(load("raw.customers", "customers.csv"))
result = runner.run(sql)
print(f"{result.row_count} rows, columns: {result.columns}")
for row in result.rows:
    print(row)
EOF
```

---

## アーキテクチャ早見図

```
PySpark コード (.py)
    │
    ▼
converter/parser.py
  PySparkParser.parse_code(source)
    → Python ast モジュールで AST 解析
    → DataFrame メソッドチェーンを Plan ツリーに変換
    → 変数名 → Plan の dict を返す
    │
    ▼  dict[str, Plan]
converter/emitter.py
  SQLEmitter.emit(plan)
    → Plan ツリーを再帰的に辿る
    → SourceTable 以外は自動で CTE (_cte_1, _cte_2, ...) に昇格
    → rules/functions.py のハンドラで関数を変換
    → rules/types.py で型名を変換
    → (sql: str, warnings: list[str]) を返す
    │
    ▼  ConversionResult
converter/transformer.py
  PySparkToBigQueryTransformer  ← ユーザーが使う唯一のクラス
    .convert_code(source)       → dict[str, ConversionResult]
    .convert_file(path)         → dict[str, ConversionResult]
    .convert_single(source)     → ConversionResult
```

---

## 重要な実装上の注意点（デバッグで判明したもの）

これらは開発時にハマったポイントです。同じ箇所を修正・拡張する際は必ず確認してください。

### parser.py

1. **`_is_spark_attr(obj, attr)` の判定**
   `obj` は `ast.Name(id='spark')` なので `isinstance(obj, ast.Name)` でチェックする。
   `ast.Attribute` と混同しないこと。

2. **`spark.read.parquet` の判定順序**
   `method in ("parquet", ...)` かつ `_is_spark_read(obj)` の順番で判定する。
   逆にすると `df.parquet(...)` のような誤検知が起きる。

3. **`agg` は `_DF_METHODS` に明示的に追加が必要**
   `_PendingGroupBy` の `.agg()` を処理するためにセットに入れておく。

4. **`_PendingGroupBy` の定義位置**
   クラス定義の前（ファイル先頭付近）に置くこと。
   `PySparkParser` クラス内で `isinstance` チェックをするため、クラス定義後だと失敗する。

5. **文字列引数のデフォルトは `Literal`、select/groupBy 内のみ `ColRef`**
   `df.select("col_a")` の `"col_a"` は `ColRef` として扱う。
   `df.filter(F.col("x") == "active")` の `"active"` は `Literal` として扱う。
   `_parse_col_expr()` と `_parse_expr()` を使い分けている。

### emitter.py

6. **`_emit_col_ref` での `"*"` の特別扱い**
   `ColRef(name="*")` は `` `*` `` ではなく `*` を返す必要がある。
   修正済みだが、`ColRef` を新しく生成するコードを書く際は注意。

7. **`UnaryOp` の `~` は `NOT` に変換**
   PySpark の `~cond` (ビット NOT) は BQ の `NOT (cond)` に変換する。
   `expr.op in ("NOT", "~")` で判定している。

8. **`_emit_filter` のソース判定**
   ソースが `SourceTable` の場合は直接 `WHERE` を付ける。
   それ以外（SelectPlan 等）は必ず CTE に昇格させてから `WHERE` を付ける。
   これをしないと `SELECT ... WHERE` が入れ子になってシンタックスエラーになる。

### rules/functions.py

9. **ハンドラの型シグネチャ**
   全ハンドラは `(args: list[str], kwargs: dict[str, str]) -> str` の形式。
   `args` は**文字列化済みの引数**（すでに emitter で `_emit_expr` 済み）。
   `handler(args, kwargs)` と **2引数**で呼ぶこと（`handler(args)` は TypeError）。

10. **`_REGISTRY` はモジュールレベルの辞書**
    `from converter.rules.functions import get_handler` でアクセスする。
    `_REGISTRY` は直接 import せず `get_handler(name)` 経由で使う。

### validator/duckdb_runner.py

11. **テーブル名の safe_name 変換**
    `"raw.orders"` → `"raw_orders"` に変換して登録する。
    SQL 中のバッククォート変換後の `"raw.orders"` → `"raw_orders"` への置換も `_adapt_sql` で行う。

12. **sqlite3 フォールバック時の型**
    sqlite3 は全カラムを TEXT で格納するため、数値比較が文字列比較になる。
    Window 関数・`SELECT * EXCEPT`・型付き集計を使うテストは `runner._engine == "sqlite"` で skip すること。

---

## Phase 3 完了内容

### `scripts/convert.py` — 単一ファイル変換 CLI

```bash
python scripts/convert.py pipeline.py                    # 全変数を stdout に出力
python scripts/convert.py pipeline.py --var result        # 特定変数のみ
python scripts/convert.py pipeline.py --out output.sql    # ファイル出力
python scripts/convert.py pipeline.py --validate          # DuckDB で構文チェック
```

- exit code: 0=成功, 1=変換エラー, 2=警告あり

### `scripts/batch_convert.py` — ディレクトリ一括変換

```bash
python scripts/batch_convert.py ./pipelines/                          # サマリー出力
python scripts/batch_convert.py ./pipelines/ --out ./sql_output/      # SQL ファイル生成
python scripts/batch_convert.py ./pipelines/ --report report.json     # JSON レポート
python scripts/batch_convert.py ./pipelines/ --recursive              # サブディレクトリ探索
```

- JSON レポート: 変換成功/失敗/警告あり件数、自動変換率、ファイルごとの詳細

### 追加サンプルパイプライン

| ファイル | 検証パターン |
|---------|-------------|
| `sample_udf.py` | format_string, year, filter+select+withColumn+groupBy |
| `sample_complex_window.py` | rowsBetween, lag, lead, rank, row_number, 累計 (running total) |
| `sample_nested_select.py` | 多段集計, UNION, dropDuplicates(subset), withColumnRenamed, 深いネスト |

### 変換率計測結果

```
4 パイプライン一括変換:
  成功:     3 (75%)
  警告あり: 1 (withColumnRenamed / dropDuplicates の情報的警告のみ、変換自体は成功)
  エラー:   0
```

---

## ファイル別の変更ガイド

### 新しい PySpark 関数に対応したい

```python
# converter/rules/functions.py に追加するだけ

@register("my_func")
def _my_func(a, k):
    return f"BQ_EQUIVALENT({a[0]})"

# 対応テストを tests/unit/test_transformer.py に追加
def test_my_func(self):
    code = """
import pyspark.sql.functions as F
df = spark.table("t")
result = df.select(F.my_func(F.col("x")).alias("y"))
"""
    result = sql(code)
    self.assertIn("BQ_EQUIVALENT(", result)
```

### 新しい DataFrame メソッドに対応したい

1. `converter/ir.py` に Plan ノードを追加（`@dataclass(frozen=True)` で不変に）
2. `converter/parser.py` の `_parse_df_method()` にケースを追加
3. `converter/emitter.py` の `_emit_plan()` と対応する `_emit_xxx()` を追加
4. テストを追加

### BQ → DuckDB の方言変換を追加したい

```python
# validator/duckdb_runner.py の _adapt_bq_to_duckdb() に追加

def _adapt_bq_to_duckdb(sql: str) -> str:
    adapted = sql
    # ... 既存の変換 ...

    # 新しい変換を追加
    adapted = re.sub(r'\bNEW_BQ_FUNC\(', 'duckdb_equivalent(', adapted)

    return adapted
```

---

## 既存コードの品質メモ

- **テスト**: 114件 (unit 64件 + integration 50件)、全件 PASS (14件 sqlite skip)
- **型ヒント**: `from __future__ import annotations` 使用、全メソッドに付与済み
- **エラーハンドリング**: 変換エラーは `ConversionResult.sql = "-- ERROR: ..."` で返す（例外を握りつぶさない）
- **警告**: BQ 非対応パターンは `/* WARNING: ... */` コメントを SQL 中に埋め込み、`ConversionResult.unsupported_patterns` にも収集
- **外部依存**: `converter/` は標準ライブラリ (`ast`, `re`, `pathlib`, `dataclasses`) のみ。`duckdb` と `pandas` はオプション

---

## よくある質問

**Q: `convert_code()` は何を返しますか？**
A: `dict[str, ConversionResult]` です。キーはソースコード中の DataFrame 変数名（例: `"result"`, `"summary"` など）です。最後の変数だけ欲しい場合は `convert_single()` を使ってください。

**Q: DuckDB がインストールされていない場合は？**
A: `validator/duckdb_runner.py` が自動的に sqlite3 でフォールバックします。Window 関数・`SELECT * EXCEPT`・型付き集計は sqlite3 非対応のため、該当テストは自動 skip されます。本番の検証には `pip install duckdb` が必要です。

**Q: PySpark の `rdd.map()` や `udf()` は変換できますか？**
A: できません。`ConversionError` または `/* WARNING: */` コメントになります。手動で BigQuery Remote Function または純粋な SQL に書き直す必要があります。

**Q: 変換結果の SQL が長い WITH 句になるのはなぜですか？**
A: `emitter.py` は DataFrame のメソッドチェーンを安全に SQL に変換するため、中間ステップを CTE に自動展開します。BigQuery のクエリオプティマイザは CTE を効率的に処理するため、実行パフォーマンス上の問題はありません。

**Q: テストは何で実行しますか？**
A: `python3 -m unittest discover -s tests -v` です。`pyproject.toml` に pytest の設定もありますが、このプロジェクトは外部ライブラリ依存なしで動くよう標準の `unittest` で書いています。pytest でも実行できます。
