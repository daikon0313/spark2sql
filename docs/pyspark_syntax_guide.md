# PySpark 構文ガイド

このガイドでは、PySpark の DataFrame API を体系的に解説します。
本プロジェクト（spark2sql）のコンバーターが対応している構文を中心に、初学者がつまずきやすいポイントも含めて説明します。

---

## 目次

1. [SparkSession の作成](#1-sparksession-の作成)
2. [データの読み込み](#2-データの読み込み)
3. [DataFrame の基本操作](#3-dataframe-の基本操作)
4. [カラムの参照方法](#4-カラムの参照方法)
5. [SELECT — カラムの選択](#5-select--カラムの選択)
6. [WHERE / FILTER — 行の絞り込み](#6-where--filter--行の絞り込み)
7. [WITH COLUMN — カラムの追加・変更](#7-with-column--カラムの追加変更)
8. [カラムの削除・リネーム](#8-カラムの削除リネーム)
9. [ORDER BY / SORT — 並び替え](#9-order-by--sort--並び替え)
10. [LIMIT — 行数制限](#10-limit--行数制限)
11. [DISTINCT — 重複除去](#11-distinct--重複除去)
12. [GROUP BY — 集計](#12-group-by--集計)
13. [JOIN — テーブル結合](#13-join--テーブル結合)
14. [UNION — テーブル結合（縦方向）](#14-union--テーブル結合縦方向)
15. [集合演算 — INTERSECT / EXCEPT](#15-集合演算--intersect--except)
16. [Window 関数](#16-window-関数)
17. [CASE WHEN — 条件分岐](#17-case-when--条件分岐)
18. [型キャスト](#18-型キャスト)
19. [NULL 処理](#19-null-処理)
20. [文字列関数](#20-文字列関数)
21. [数値関数](#21-数値関数)
22. [日付・時刻関数](#22-日付時刻関数)
23. [集計関数](#23-集計関数)
24. [配列・構造体・JSON](#24-配列構造体json)
25. [その他の DataFrame 操作](#25-その他の-dataframe-操作)
26. [UDF（ユーザー定義関数）](#26-udfユーザー定義関数)
27. [RDD 操作（参考）](#27-rdd-操作参考)
28. [よくあるエラーと対処法](#28-よくあるエラーと対処法)

---

## 1. SparkSession の作成

PySpark を使うには、まず `SparkSession` を作成します。すべての処理の起点になるオブジェクトです。

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()
```

| 設定 | 説明 |
|------|------|
| `.appName("名前")` | アプリケーション名（Spark UI に表示される） |
| `.master("local[*]")` | ローカルモード（全 CPU コア使用） |
| `.master("spark://host:7077")` | クラスタモード |
| `.config("key", "value")` | Spark の設定を追加 |
| `.getOrCreate()` | 既存セッションがあれば再利用、なければ新規作成 |

### ローカル開発での典型パターン

```python
spark = SparkSession.builder \
    .appName("LocalDev") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```

> **ポイント**: `local[*]` は開発時に使います。本番では Spark クラスタの URL を指定します。

---

## 2. データの読み込み

### CSV ファイル

```python
df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)

# オプションを個別に指定
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .csv("data/orders.csv")
```

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `header` | 1行目をヘッダーとして使う | `false` |
| `inferSchema` | 型を自動推論する | `false`（全て文字列になる） |
| `delimiter` | 区切り文字 | `,` |
| `encoding` | 文字エンコーディング | `UTF-8` |

### Parquet ファイル

```python
df = spark.read.parquet("data/orders.parquet")
```

> Parquet はスキーマ情報を含むため、`inferSchema` は不要です。

### JSON ファイル

```python
df = spark.read.json("data/orders.json")

# 複数行 JSON
df = spark.read.option("multiLine", "true").json("data/orders.json")
```

### テーブルから読み込み

```python
df = spark.table("database.table_name")

# SQL を直接実行
df = spark.sql("SELECT * FROM database.table_name WHERE status = 'active'")
```

### 汎用フォーマット

```python
df = spark.read.format("parquet").load("data/orders.parquet")
df = spark.read.format("csv").option("header", "true").load("data/orders.csv")
df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://host:3306/db") \
    .option("dbtable", "orders") \
    .load()
```

### Python リストから DataFrame を作成

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# スキーマ指定あり
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])
data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, schema)

# 簡易作成（型推論）
df = spark.createDataFrame([
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
])
```

---

## 3. DataFrame の基本操作

DataFrame を作成したら、まず中身を確認しましょう。

```python
# 先頭 20 行を表示
df.show()

# 先頭 5 行を表示
df.show(5)

# カラムが長い場合に省略しない
df.show(truncate=False)

# スキーマ（カラム名と型）を確認
df.printSchema()

# 行数を数える
df.count()

# カラム名一覧
df.columns        # ['order_id', 'customer_id', 'amount', ...]

# データ型一覧
df.dtypes         # [('order_id', 'int'), ('customer_id', 'int'), ...]

# 統計情報
df.describe().show()   # count, mean, stddev, min, max
df.summary().show()    # describe + 25%, 50%, 75%
```

---

## 4. カラムの参照方法

PySpark ではカラムを参照する方法が複数あります。

```python
from pyspark.sql import functions as F

# 方法1: F.col() — 最も推奨
F.col("name")

# 方法2: df["name"] — DataFrame に紐づく
df["name"]

# 方法3: df.name — ドット記法（短いが、予約語と衝突する可能性あり）
df.name

# 方法4: 文字列 — .select() や .groupBy() の引数として
df.select("name", "age")
```

> **推奨**: `F.col("name")` を使いましょう。DataFrame に依存せず、どこでも使えます。

### `F.col()` と `F.lit()` の違い

```python
F.col("price")    # カラム「price」の値を参照
F.lit(100)        # 定数値 100 を全行に適用

# 例: 全行に固定値のカラムを追加
df.withColumn("country", F.lit("JP"))
```

---

## 5. SELECT — カラムの選択

必要なカラムだけを選択します。SQL の `SELECT` に相当します。

```python
# 文字列で指定
df.select("name", "age")

# F.col() で指定
df.select(F.col("name"), F.col("age"))

# 全カラム
df.select("*")

# 計算カラムを追加
df.select(
    "name",
    "age",
    (F.col("age") + 1).alias("next_age"),  # age + 1 を next_age として出力
)

# selectExpr — SQL 式を文字列で書ける
df.selectExpr(
    "name",
    "age",
    "age + 1 as next_age",
    "UPPER(name) as upper_name",
)
```

### `.alias()` — カラムに別名をつける

```python
F.col("total_amount").alias("revenue")
F.sum("amount").alias("total")
```

---

## 6. WHERE / FILTER — 行の絞り込み

条件に合う行だけを残します。`where()` と `filter()` は完全に同じです。

```python
# 基本的な比較
df.filter(F.col("age") > 20)
df.where(F.col("status") == "active")

# 文字列で SQL 式を書くこともできる
df.filter("age > 20")
df.where("status = 'active'")
```

### 比較演算子

```python
F.col("age") == 30        # 等しい
F.col("age") != 30        # 等しくない
F.col("age") > 30         # より大きい
F.col("age") >= 30        # 以上
F.col("age") < 30         # より小さい
F.col("age") <= 30        # 以下
```

### 論理演算子

```python
# AND: &（ビット演算子）
df.filter((F.col("age") > 20) & (F.col("status") == "active"))

# OR: |
df.filter((F.col("age") > 60) | (F.col("status") == "vip"))

# NOT: ~
df.filter(~(F.col("status") == "inactive"))
```

> **重要**: `&` `|` `~` を使うときは、各条件を必ず `()` で囲んでください。
> Python の演算子優先順位の関係で、括弧がないと意図しない結果になります。

### よく使うフィルター条件

```python
# NULL チェック
df.filter(F.col("email").isNotNull())
df.filter(F.col("email").isNull())

# IN リスト
df.filter(F.col("status").isin("active", "pending"))
df.filter(F.col("status").isin(["active", "pending"]))  # リストでも OK

# LIKE（部分一致）
df.filter(F.col("name").like("%田%"))         # 「田」を含む
df.filter(F.col("name").startswith("山"))     # 「山」で始まる
df.filter(F.col("name").endswith("郎"))       # 「郎」で終わる

# BETWEEN
df.filter(F.col("age").between(20, 30))

# contains（文字列を含む）
df.filter(F.col("name").contains("太"))
```

---

## 7. WITH COLUMN — カラムの追加・変更

既存の DataFrame に新しいカラムを追加、または既存カラムを上書きします。

```python
# 新しいカラムを追加
df = df.withColumn("tax", F.col("amount") * 0.1)

# 既存カラムを上書き（同じ名前を指定）
df = df.withColumn("amount", F.col("amount") * 1.1)

# 定数カラムを追加
df = df.withColumn("country", F.lit("JP"))

# 条件付きカラム
df = df.withColumn(
    "category",
    F.when(F.col("amount") > 10000, "high")
     .when(F.col("amount") > 5000, "medium")
     .otherwise("low")
)
```

### 複数カラムを一度に追加

```python
# チェーンで連続追加
df = df \
    .withColumn("tax", F.col("amount") * 0.1) \
    .withColumn("total", F.col("amount") + F.col("tax"))
```

> **注意**: `withColumn` は毎回新しい DataFrame を返します（元の df は変わりません）。
> PySpark の DataFrame は**不変 (immutable)** です。

---

## 8. カラムの削除・リネーム

### カラムの削除

```python
# 1つ削除
df = df.drop("unnecessary_column")

# 複数削除
df = df.drop("col1", "col2", "col3")
```

### カラム名の変更

```python
# 1つリネーム
df = df.withColumnRenamed("old_name", "new_name")

# 複数リネーム（チェーン）
df = df \
    .withColumnRenamed("total_amount", "revenue") \
    .withColumnRenamed("customer_name", "client")
```

### 全カラム名を一括変更

```python
# toDF で全カラム名を一括指定
df = df.toDF("id", "name", "price", "quantity")
```

---

## 9. ORDER BY / SORT — 並び替え

```python
# 昇順（デフォルト）
df.orderBy("amount")
df.sort("amount")           # orderBy と同じ

# 降順
df.orderBy(F.col("amount").desc())
df.orderBy(F.desc("amount"))

# 複数キー
df.orderBy(
    F.col("status").asc(),
    F.col("amount").desc(),
)

# NULL の扱い
F.col("amount").asc_nulls_first()    # NULL を先頭に
F.col("amount").asc_nulls_last()     # NULL を末尾に
F.col("amount").desc_nulls_first()
F.col("amount").desc_nulls_last()
```

---

## 10. LIMIT — 行数制限

```python
df = df.limit(100)   # 先頭 100 行だけ取得
```

---

## 11. DISTINCT — 重複除去

```python
# 全カラムで重複除去
df = df.distinct()

# 特定カラムで重複除去（各グループの先頭 1 行を残す）
df = df.dropDuplicates(["customer_id"])
df = df.drop_duplicates(["customer_id"])  # エイリアス
```

> **補足**: `dropDuplicates(subset)` は SQL では `ROW_NUMBER() OVER (PARTITION BY ...)` を使って
> 各グループの 1 行目だけを残す処理に変換されます。

---

## 12. GROUP BY — 集計

### 基本の集計

```python
from pyspark.sql import functions as F

# 1カラムで集計
df.groupBy("department").agg(
    F.count("*").alias("cnt"),
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("avg_salary"),
)

# 複数カラムで集計
df.groupBy("department", "job_title").agg(
    F.count("*").alias("cnt"),
)

# ショートカット（1つの集計のみ）
df.groupBy("department").count()
df.groupBy("department").sum("salary")
df.groupBy("department").avg("salary")
df.groupBy("department").min("salary")
df.groupBy("department").max("salary")
```

### 主要な集計関数

```python
F.count("*")              # 行数
F.count("col")            # NULL でない行数
F.countDistinct("col")    # ユニーク数
F.sum("col")              # 合計
F.avg("col")              # 平均
F.min("col")              # 最小値
F.max("col")              # 最大値
F.first("col")            # 最初の値（非決定的）
F.last("col")             # 最後の値（非決定的）
F.collect_list("col")     # 値をリストに集約
F.collect_set("col")      # 値をユニークリストに集約
F.stddev("col")           # 標準偏差（サンプル）
F.variance("col")         # 分散（サンプル）
F.approxCountDistinct("col")  # 近似ユニーク数（高速）
```

### ROLLUP / CUBE

通常の GROUP BY に加えて、小計・総計を出す機能です。

```python
# ROLLUP: 階層的な小計 + 総計
df.rollup("department", "job_title").agg(
    F.sum("salary").alias("total"),
)
# 結果: (dept, job), (dept, NULL), (NULL, NULL) の 3 レベル

# CUBE: 全ての組み合わせの小計
df.cube("department", "job_title").agg(
    F.sum("salary").alias("total"),
)
# 結果: (dept, job), (dept, NULL), (NULL, job), (NULL, NULL) の 4 レベル
```

### PIVOT — ピボットテーブル

行の値をカラムに展開します。

```python
df.groupBy("department") \
    .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]) \
    .agg(F.sum("revenue"))
```

結果イメージ:

| department | Q1 | Q2 | Q3 | Q4 |
|-----------|-----|-----|-----|-----|
| Sales | 100 | 150 | 200 | 180 |
| Engineering | 80 | 90 | 110 | 120 |

> **ポイント**: `.pivot()` の第 2 引数に値リストを指定すると、パフォーマンスが向上します。

---

## 13. JOIN — テーブル結合

### 基本構文

```python
result = df_orders.join(
    df_customers,                          # 結合先 DataFrame
    on=df_orders["customer_id"] == df_customers["customer_id"],  # 結合条件
    how="inner",                           # 結合タイプ
)
```

### 結合タイプ一覧

| `how` の値 | 説明 | SQL 相当 |
|-----------|------|---------|
| `"inner"` | 両方に存在する行のみ（デフォルト） | `INNER JOIN` |
| `"left"` / `"left_outer"` | 左テーブルの全行を保持 | `LEFT OUTER JOIN` |
| `"right"` / `"right_outer"` | 右テーブルの全行を保持 | `RIGHT OUTER JOIN` |
| `"full"` / `"full_outer"` / `"outer"` | 両方の全行を保持 | `FULL OUTER JOIN` |
| `"left_semi"` | 右に存在する左の行のみ（右のカラムは含まない） | `WHERE EXISTS (...)` |
| `"left_anti"` | 右に存在しない左の行のみ | `WHERE NOT EXISTS (...)` |
| `"cross"` | 全行の組み合わせ（直積） | `CROSS JOIN` |

### 結合条件の書き方

```python
# 同名カラムで結合（推奨：カラムの重複が起きない）
df1.join(df2, on="customer_id", how="inner")
df1.join(df2, on=["customer_id", "order_date"], how="inner")

# 異なるカラム名で結合
df1.join(df2, on=df1["cust_id"] == df2["customer_id"], how="left")

# 複合条件
df1.join(
    df2,
    on=(df1["id"] == df2["id"]) & (df1["date"] == df2["date"]),
    how="inner",
)

# CROSS JOIN（条件なし）
df1.crossJoin(df2)
```

### JOIN 後のカラム重複に注意

```python
# ❌ 同名カラムがあると曖昧になる
result = df1.join(df2, on=df1["id"] == df2["id"])
result.select("id")  # エラー: どちらの id かわからない

# ✅ 対策1: 文字列で結合キーを指定
result = df1.join(df2, on="id")  # 自動で 1 つに統合される

# ✅ 対策2: 片方を drop
result = df1.join(df2, on=df1["id"] == df2["id"]).drop(df2["id"])

# ✅ 対策3: alias を使う
df1_a = df1.alias("a")
df2_b = df2.alias("b")
result = df1_a.join(df2_b, on=F.col("a.id") == F.col("b.id"))
```

---

## 14. UNION — テーブル結合（縦方向）

2つの DataFrame を縦に連結します。

```python
# UNION ALL（重複を許可）
result = df1.union(df2)
result = df1.unionAll(df2)        # 同じ

# カラム名で合わせて結合（順序が異なっても OK）
result = df1.unionByName(df2)

# 欠損カラムを NULL で埋める
result = df1.unionByName(df2, allowMissingColumns=True)
```

> **注意**: `union()` はカラムの**位置**で結合します。カラム順序が異なると間違った結果になります。
> `unionByName()` を使うほうが安全です。

---

## 15. 集合演算 — INTERSECT / EXCEPT

```python
# 両方に存在する行（重複除去あり）
result = df1.intersect(df2)

# 両方に存在する行（重複除去なし）
result = df1.intersectAll(df2)

# df1 にあって df2 にない行（重複除去あり）
result = df1.subtract(df2)

# df1 にあって df2 にない行（重複除去なし）
result = df1.exceptAll(df2)
```

---

## 16. Window 関数

Window 関数は、GROUP BY のように行をグループ化しつつ、元の行数を保ったまま計算できる強力な機能です。

### Window の定義

```python
from pyspark.sql.window import Window

# PARTITION BY + ORDER BY
w = Window.partitionBy("department").orderBy("salary")

# PARTITION BY のみ
w = Window.partitionBy("department")

# ORDER BY のみ
w = Window.orderBy("date")
```

### Window 関数の使い方

```python
# ランキング系
df.withColumn("rank", F.rank().over(w))
df.withColumn("dense_rank", F.dense_rank().over(w))
df.withColumn("row_number", F.row_number().over(w))
df.withColumn("ntile", F.ntile(4).over(w))

# 前後の行の値
df.withColumn("prev_salary", F.lag("salary", 1).over(w))
df.withColumn("next_salary", F.lead("salary", 1).over(w))
df.withColumn("prev_salary", F.lag("salary", 1, 0).over(w))  # デフォルト値つき

# 集計の Window 関数版（行を減らさない集計）
w_dept = Window.partitionBy("department")
df.withColumn("dept_avg", F.avg("salary").over(w_dept))
df.withColumn("dept_sum", F.sum("salary").over(w_dept))
df.withColumn("dept_count", F.count("*").over(w_dept))

# 累計
w_cumul = Window.partitionBy("department").orderBy("date").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df.withColumn("running_total", F.sum("amount").over(w_cumul))

# 最初の値・最後の値
df.withColumn("first_val", F.first_value("salary").over(w))
df.withColumn("last_val", F.last_value("salary").over(w))
```

### Window Frame（範囲指定）

```python
# ROWS BETWEEN: 行数ベースの範囲
w = Window.partitionBy("dept").orderBy("date").rowsBetween(-2, 0)
# → 現在行の 2 行前 ～ 現在行

# RANGE BETWEEN: 値ベースの範囲
w = Window.partitionBy("dept").orderBy("date").rangeBetween(
    Window.unboundedPreceding, Window.currentRow
)

# 特殊な値
Window.unboundedPreceding  # パーティションの先頭
Window.unboundedFollowing  # パーティションの末尾
Window.currentRow          # 現在行（= 0）
```

### 典型的な使用パターン

```python
# パターン1: 各グループの最新レコードを取得
w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
df_latest = df \
    .withColumn("rn", F.row_number().over(w)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

# パターン2: 前月との差分
w = Window.partitionBy("product_id").orderBy("month")
df.withColumn("mom_diff", F.col("revenue") - F.lag("revenue", 1).over(w))

# パターン3: グループ内の割合
w = Window.partitionBy("department")
df.withColumn("pct", F.col("salary") / F.sum("salary").over(w))
```

---

## 17. CASE WHEN — 条件分岐

SQL の `CASE WHEN ... THEN ... ELSE ... END` に相当します。

```python
# 基本
result = F.when(F.col("age") >= 65, "senior") \
          .when(F.col("age") >= 18, "adult") \
          .otherwise("minor")

df.withColumn("category", result)

# 条件に合致しない場合は NULL（otherwise を省略した場合）
F.when(F.col("score") >= 80, "pass")
# → score < 80 のとき NULL

# 複雑な条件
F.when(
    (F.col("status") == "active") & (F.col("balance") > 0),
    "good"
).when(
    F.col("status") == "active",
    "warning"
).otherwise("inactive")
```

---

## 18. 型キャスト

カラムのデータ型を変換します。

```python
# 文字列で型を指定
df.withColumn("amount", F.col("amount").cast("double"))
df.withColumn("id", F.col("id").cast("string"))

# 型オブジェクトで指定
from pyspark.sql.types import IntegerType, DoubleType
df.withColumn("count", F.col("count").cast(IntegerType()))
```

### よく使う型名

| 型名（文字列） | 説明 |
|--------------|------|
| `"string"` | 文字列 |
| `"int"` / `"integer"` | 32ビット整数 |
| `"long"` / `"bigint"` | 64ビット整数 |
| `"float"` | 32ビット浮動小数点 |
| `"double"` | 64ビット浮動小数点 |
| `"boolean"` | 真偽値 |
| `"date"` | 日付 |
| `"timestamp"` | 日時 |
| `"decimal(18,3)"` | 固定小数点 |
| `"binary"` | バイナリ |

---

## 19. NULL 処理

### NULL の判定

```python
df.filter(F.col("email").isNull())       # NULL の行
df.filter(F.col("email").isNotNull())    # NULL でない行
```

### NULL の補完

```python
# 全カラムの NULL を補完
df.fillna(0)                  # 数値カラムの NULL を 0 に
df.fillna("unknown")          # 文字列カラムの NULL を "unknown" に

# 特定カラムのみ
df.fillna(0, subset=["amount", "quantity"])
df.fillna({"amount": 0, "name": "unknown"})

# na アクセサ経由
df.na.fill(0)
df.na.fill({"amount": 0, "status": "pending"})
```

### NULL 行の削除

```python
# いずれかのカラムが NULL の行を削除（デフォルト）
df.dropna()
df.dropna(how="any")

# 全カラムが NULL の行を削除
df.dropna(how="all")

# 特定カラムのみチェック
df.dropna(subset=["name", "email"])

# na アクセサ経由
df.na.drop(how="any", subset=["name"])
```

### COALESCE — 最初の非 NULL 値

```python
# a が NULL なら b、b も NULL なら c
F.coalesce(F.col("a"), F.col("b"), F.col("c"))

# NULL なら固定値
F.coalesce(F.col("name"), F.lit("名無し"))
```

### その他の NULL 関数

```python
F.nullif(F.col("a"), F.col("b"))    # a == b なら NULL、そうでなければ a
F.nvl(F.col("a"), F.col("b"))      # a が NULL なら b（= COALESCE の 2 引数版）
```

---

## 20. 文字列関数

```python
from pyspark.sql import functions as F
```

### 大文字・小文字変換

```python
F.upper(F.col("name"))         # 大文字に
F.lower(F.col("name"))         # 小文字に
F.initcap(F.col("name"))       # 先頭を大文字に（"hello world" → "Hello World"）
```

### 空白除去

```python
F.trim(F.col("name"))          # 両端の空白除去
F.ltrim(F.col("name"))         # 左の空白除去
F.rtrim(F.col("name"))         # 右の空白除去
```

### 文字列の切り出し・結合

```python
F.substring(F.col("phone"), 1, 3)       # 1文字目から 3 文字取得
F.substr(F.col("phone"), 1, 3)          # 同上

F.concat(F.col("first"), F.col("last"))                   # 結合
F.concat_ws("-", F.col("year"), F.col("month"), F.col("day"))  # 区切り文字つき結合
```

### 検索・置換

```python
F.instr(F.col("text"), "target")                    # 位置を返す（1始まり、見つからない場合 0）
F.locate("target", F.col("text"))                   # 同上

F.regexp_replace(F.col("text"), r"\d+", "NUM")      # 正規表現で置換
F.regexp_extract(F.col("text"), r"(\d+)", 1)        # 正規表現で抽出（グループ 1）

F.split(F.col("tags"), ",")                         # 区切り文字で分割 → 配列
```

### パディング・長さ

```python
F.lpad(F.col("id"), 5, "0")    # 左詰め（"42" → "00042"）
F.rpad(F.col("id"), 5, "0")    # 右詰め（"42" → "42000"）
F.length(F.col("name"))        # 文字数
```

### その他

```python
F.reverse(F.col("text"))                    # 逆順
F.repeat(F.col("char"), 3)                  # 繰り返し
F.format_string("%s: %d", F.col("name"), F.col("age"))  # printf 形式
```

---

## 21. 数値関数

```python
F.abs(F.col("diff"))           # 絶対値
F.ceil(F.col("price"))         # 切り上げ（3.2 → 4）
F.floor(F.col("price"))        # 切り捨て（3.8 → 3）
F.round(F.col("price"), 2)     # 四捨五入（小数点以下 2 桁）
F.bround(F.col("price"), 2)    # 銀行丸め

F.pow(F.col("base"), 2)        # べき乗（base²）
F.sqrt(F.col("value"))         # 平方根
F.cbrt(F.col("value"))         # 立方根
F.exp(F.col("x"))              # e^x
F.log(F.col("x"))              # 自然対数 ln(x)
F.log(10, F.col("x"))          # 対数 log₁₀(x)
F.log2(F.col("x"))             # 対数 log₂(x)
F.log10(F.col("x"))            # 対数 log₁₀(x)

F.greatest(F.col("a"), F.col("b"), F.col("c"))  # 最大値
F.least(F.col("a"), F.col("b"), F.col("c"))     # 最小値

F.rand()                       # 乱数 [0, 1)
F.rand(42)                     # シード付き乱数
F.randn()                      # 正規分布乱数

F.pmod(F.col("a"), F.col("b"))    # 正の剰余（Python の % と異なり常に正）
F.signum(F.col("x"))              # 符号（-1, 0, 1）
```

---

## 22. 日付・時刻関数

### 現在日時

```python
F.current_date()               # 今日の日付（DATE 型）
F.current_timestamp()          # 現在日時（TIMESTAMP 型）
```

### 文字列 → 日付/日時

```python
F.to_date(F.col("date_str"), "yyyy-MM-dd")            # 文字列 → DATE
F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss")  # 文字列 → TIMESTAMP
```

### 日付/日時 → 文字列

```python
F.date_format(F.col("date"), "yyyy/MM/dd")             # DATE → 文字列
```

### 日付フォーマット文字列

| 記号 | 意味 | 例 |
|------|------|-----|
| `yyyy` | 年（4桁） | 2026 |
| `MM` | 月（2桁） | 03 |
| `dd` | 日（2桁） | 12 |
| `HH` | 時（24時間、2桁） | 14 |
| `mm` | 分（2桁） | 30 |
| `ss` | 秒（2桁） | 45 |

### 日付の加減算

```python
F.date_add(F.col("date"), 7)       # 7 日後
F.date_sub(F.col("date"), 7)       # 7 日前
F.add_months(F.col("date"), 3)     # 3 ヶ月後
```

### 日付差分

```python
F.datediff(F.col("end_date"), F.col("start_date"))      # 日数差
F.months_between(F.col("end_date"), F.col("start_date")) # 月数差（小数）
```

### 日付の切り捨て

```python
F.trunc(F.col("date"), "MM")       # 月初に切り捨て
F.trunc(F.col("date"), "yyyy")     # 年初に切り捨て
F.date_trunc("month", F.col("ts")) # TIMESTAMP 版
```

### 要素の抽出

```python
F.year(F.col("date"))              # 年
F.month(F.col("date"))             # 月
F.dayofmonth(F.col("date"))        # 日
F.dayofweek(F.col("date"))         # 曜日（1=日, 2=月, ..., 7=土）
F.dayofyear(F.col("date"))         # 年内の日番号
F.weekofyear(F.col("date"))        # 年内の週番号
F.quarter(F.col("date"))           # 四半期（1-4）
F.hour(F.col("ts"))                # 時
F.minute(F.col("ts"))              # 分
F.second(F.col("ts"))              # 秒
```

### UNIX タイムスタンプ

```python
F.unix_timestamp(F.col("ts"))              # TIMESTAMP → UNIX 秒
F.from_unixtime(F.col("epoch_sec"))        # UNIX 秒 → 文字列
F.from_unixtime(F.col("epoch_sec"), "yyyy-MM-dd")  # フォーマット付き
```

---

## 23. 集計関数

集計関数は `.groupBy().agg()` の中で使います（Window 関数としても使えるものが多い）。

```python
# 基本集計
F.count("*")                           # 全行数
F.count("col")                         # NULL でない行数
F.countDistinct("col")                 # ユニーク数
F.sum("col")                           # 合計
F.avg("col")                           # 平均
F.mean("col")                          # 平均（avg と同じ）
F.min("col")                           # 最小
F.max("col")                           # 最大

# 統計
F.stddev("col")                        # 標準偏差（サンプル）
F.stddev_pop("col")                    # 標準偏差（母集団）
F.variance("col")                      # 分散（サンプル）
F.var_pop("col")                       # 分散（母集団）

# 集約
F.collect_list("col")                  # 値をリストに（重複あり、NULL 含む）
F.collect_set("col")                   # 値をリストに（重複なし）
F.first("col")                         # 最初の値（非決定的）
F.last("col")                          # 最後の値（非決定的）

# 近似
F.approxCountDistinct("col")           # 近似ユニーク数（高速）
F.approx_count_distinct("col")         # エイリアス
F.percentile_approx("col", 0.5)        # 近似中央値
```

---

## 24. 配列・構造体・JSON

### 配列

```python
# 配列の作成
F.array(F.col("a"), F.col("b"), F.col("c"))

# 配列の操作
F.array_contains(F.col("tags"), "python")  # 含まれるか
F.size(F.col("tags"))                      # 要素数
F.array_join(F.col("tags"), ", ")          # 配列 → 文字列
F.sort_array(F.col("tags"))               # ソート
F.array_distinct(F.col("tags"))           # 重複除去
F.flatten(F.col("nested_array"))          # ネスト解除
F.array_union(F.col("a"), F.col("b"))     # 和集合
F.array_intersect(F.col("a"), F.col("b")) # 積集合
F.array_except(F.col("a"), F.col("b"))    # 差集合
```

### EXPLODE — 配列の展開

```python
# 配列の各要素を 1 行ずつに展開
df.select("id", F.explode("tags").alias("tag"))

# NULL / 空配列の行も保持
df.select("id", F.explode_outer("tags").alias("tag"))

# インデックス付き
df.select("id", F.posexplode("tags").alias("pos", "tag"))
```

> **注意**: explode は BigQuery では `UNNEST` に変換されます。

### 構造体（STRUCT）

```python
# 構造体の作成
F.struct(F.col("name"), F.col("age"))

# フィールドのアクセス
F.col("address.city")          # ドット記法
F.col("address")["city"]       # ブラケット記法
```

### JSON

```python
F.get_json_object(F.col("json_str"), "$.name")      # JSON から値を取得
F.from_json(F.col("json_str"), schema)               # JSON → 構造体
F.to_json(F.col("struct_col"))                        # 構造体 → JSON 文字列
```

---

## 25. その他の DataFrame 操作

### サンプリング

```python
df.sample(fraction=0.1)           # 10% のランダムサンプル
df.sample(fraction=0.1, seed=42)  # 再現性のあるサンプル
```

### 値の置換

```python
# 特定の値を置換
df.replace({"M": "Male", "F": "Female"}, subset=["gender"])

# na アクセサ経由
df.na.replace({"M": "Male", "F": "Female"}, subset=["gender"])
```

### UNPIVOT（列→行変換）

pivot の逆で、複数カラムを行に展開します。

```python
# Spark 3.4+
df.unpivot(
    ids=["id", "name"],              # そのまま残すカラム
    values=["Q1", "Q2", "Q3", "Q4"],  # 行に展開するカラム
    variableColumnName="quarter",     # 展開したカラム名が入る列名
    valueColumnName="revenue",        # 展開した値が入る列名
)

# melt（エイリアス）
df.melt(
    ids=["id", "name"],
    values=["Q1", "Q2", "Q3", "Q4"],
    variableColumnName="quarter",
    valueColumnName="revenue",
)
```

### crosstab（クロス集計）

```python
df.crosstab("department", "gender")
```

### cache / persist — キャッシュ

```python
df.cache()                   # メモリにキャッシュ
df.persist()                 # ストレージレベルを指定してキャッシュ
df.unpersist()               # キャッシュを解放
```

> BigQuery では不要です（自動的にキャッシュされます）。

### broadcast — ブロードキャストヒント

```python
from pyspark.sql.functions import broadcast

# 小さいテーブルをブロードキャスト JOIN
result = df_large.join(broadcast(df_small), on="id")
```

> BigQuery では不要です（オプティマイザが自動判断します）。

### repartition / coalesce

```python
df.repartition(10)              # パーティション数を変更（シャッフルあり）
df.repartition("department")    # 特定カラムで再パーティション
df.coalesce(1)                  # パーティション数を減らす（シャッフルなし）
```

> BigQuery では不要です（ストレージとコンピュートが分離されているため）。

---

## 26. UDF（ユーザー定義関数）

Spark の組み込み関数で実現できない処理がある場合、Python の関数を UDF として登録できます。

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# UDF の定義
@udf(returnType=StringType())
def to_greeting(name):
    return f"Hello, {name}!"

# 使用
df.withColumn("greeting", to_greeting(F.col("name")))
```

### Pandas UDF（高速版）

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def multiply(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b

df.withColumn("result", multiply(F.col("price"), F.col("quantity")))
```

> **注意**: UDF は BigQuery SQL に自動変換できません。
> 本プロジェクトのコンバーターは UDF を検出すると警告を出します。
> 可能な限り組み込み関数で書き直すことを推奨します。

---

## 27. RDD 操作（参考）

DataFrame の前身である RDD (Resilient Distributed Dataset) の操作です。
現在は DataFrame API の使用が推奨されていますが、参考として記載します。

```python
rdd = df.rdd

# 変換
rdd.map(lambda row: (row.name, row.age * 2))
rdd.flatMap(lambda row: row.tags)
rdd.filter(lambda row: row.age > 20)
rdd.distinct()

# アクション
rdd.collect()          # 全データを取得（大量データに注意）
rdd.take(5)            # 先頭 5 件
rdd.count()            # 行数
rdd.first()            # 先頭 1 件
```

> **注意**: RDD 操作は SQL に変換できません。
> DataFrame API で書き直す必要があります。

---

## 28. よくあるエラーと対処法

### 1. `AnalysisException: Column 'xxx' does not exist`

```python
# ❌ カラム名のタイポ
df.select("naem")  # name のタイポ

# ✅ columns で確認
print(df.columns)
```

### 2. `AnalysisException: Reference 'xxx' is ambiguous`

JOIN 後に同名カラムが複数ある場合に発生します。

```python
# ❌ 曖昧な参照
df1.join(df2, on=df1["id"] == df2["id"]).select("id")

# ✅ テーブルを指定
df1.join(df2, on="id").select("id")
```

### 3. 演算子の優先順位エラー

```python
# ❌ 括弧なし → エラーや意図しない結果
df.filter(F.col("a") > 1 & F.col("b") > 2)

# ✅ 各条件に括弧をつける
df.filter((F.col("a") > 1) & (F.col("b") > 2))
```

### 4. `Py4JJavaError: ... NullPointerException`

DataFrame の操作中に NULL が原因でエラーになる場合。

```python
# ✅ NULL を事前に処理
df = df.fillna(0, subset=["amount"])
# または
df = df.filter(F.col("amount").isNotNull())
```

### 5. 型の不一致

```python
# ❌ 文字列と数値の比較
df.filter(F.col("id") == "100")  # id が IntegerType の場合

# ✅ 型を合わせる
df.filter(F.col("id") == 100)
df.filter(F.col("id") == F.lit(100))
```

---

## ハッシュ関数

```python
F.md5(F.col("text"))             # MD5 ハッシュ
F.sha1(F.col("text"))            # SHA-1 ハッシュ
F.sha2(F.col("text"), 256)       # SHA-256 ハッシュ
F.hash(F.col("col1"), F.col("col2"))  # MurmurHash3
F.xxhash64(F.col("col1"))        # xxHash64
```

---

## 条件付き関数

```python
F.coalesce(F.col("a"), F.col("b"))        # 最初の非 NULL
F.nullif(F.col("a"), F.col("b"))          # a == b なら NULL
F.nvl(F.col("a"), F.col("b"))             # a が NULL なら b
F.nanvl(F.col("a"), F.col("b"))           # a が NaN なら b
F.ifnull(F.col("a"), F.col("b"))          # nvl と同じ
```

---

## データ型一覧

PySpark で使用可能な主なデータ型です。

```python
from pyspark.sql.types import *
```

| 型 | 説明 | Python の対応型 |
|----|------|----------------|
| `StringType()` | 文字列 | `str` |
| `IntegerType()` | 32ビット整数 | `int` |
| `LongType()` | 64ビット整数 | `int` |
| `FloatType()` | 32ビット浮動小数点 | `float` |
| `DoubleType()` | 64ビット浮動小数点 | `float` |
| `BooleanType()` | 真偽値 | `bool` |
| `DateType()` | 日付 | `datetime.date` |
| `TimestampType()` | 日時 | `datetime.datetime` |
| `DecimalType(p, s)` | 固定小数点（精度 p, スケール s） | `decimal.Decimal` |
| `BinaryType()` | バイナリ | `bytes` |
| `ArrayType(elem)` | 配列 | `list` |
| `MapType(key, val)` | マップ | `dict` |
| `StructType([...])` | 構造体 | `Row` |

### スキーマ定義の例

```python
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("scores", ArrayType(IntegerType()), nullable=True),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("zip", StringType()),
    ])),
])
```

---

## データの書き出し

```python
# CSV
df.write.csv("output/data.csv", header=True, mode="overwrite")

# Parquet
df.write.parquet("output/data.parquet", mode="overwrite")

# JSON
df.write.json("output/data.json", mode="overwrite")

# テーブルに保存
df.write.saveAsTable("database.table_name", mode="overwrite")

# パーティション付き書き出し
df.write.partitionBy("year", "month").parquet("output/data.parquet")
```

### 書き込みモード

| モード | 説明 |
|--------|------|
| `"overwrite"` | 既存データを上書き |
| `"append"` | 既存データに追記 |
| `"ignore"` | 既にデータがある場合は何もしない |
| `"error"` / `"errorifexists"` | 既にデータがあればエラー（デフォルト） |

---

## PySpark → BigQuery SQL の変換について

本プロジェクト（spark2sql）では、上記の PySpark コードを BigQuery 標準 SQL に自動変換します。

### 変換例

**PySpark:**
```python
df = spark.table("raw.orders")
result = df \
    .filter(F.col("status") == "active") \
    .groupBy("customer_id") \
    .agg(
        F.sum("amount").alias("total"),
        F.count("*").alias("cnt"),
    ) \
    .orderBy(F.col("total").desc())
```

**生成される BigQuery SQL:**
```sql
WITH
_cte_1 AS (
  SELECT * FROM `raw.orders`
  WHERE status = 'active'
),
_cte_2 AS (
  SELECT
    customer_id,
    SUM(amount) AS total,
    COUNT(*) AS cnt
  FROM _cte_1
  GROUP BY customer_id
)
SELECT * FROM _cte_2
ORDER BY total DESC
```

### 変換コマンド

```bash
# 単一ファイル変換
python scripts/convert.py my_pipeline.py

# ディレクトリ一括変換
python scripts/batch_convert.py base_code/ --out converted_code/
```

詳しくは [README.md](../README.md) を参照してください。
