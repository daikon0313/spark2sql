"""
Spark Runner

PySpark スクリプトを Spark で実行して QueryResult を返す。
Docker 環境 (spark-master コンテナ) または spark-submit CLI を使う。

ローカル Docker なし環境では spark_runner は実行せず、
DuckDB 単体 or SQL レビューモードで検証する。
"""
from __future__ import annotations

import subprocess
import json
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from .duckdb_runner import QueryResult


@dataclass
class SparkConfig:
    """Spark 接続設定"""
    master: str = "spark://localhost:7077"   # Docker 環境
    app_name: str = "pyspark-bq-validator"
    executor_memory: str = "1g"
    driver_memory: str = "1g"
    # local モード (Docker なし)
    use_local: bool = True                   # True = local[*] で実行


class SparkRunner:
    """
    PySpark スクリプトを実行して結果を QueryResult として返す。

    Docker 環境:
        config = SparkConfig(master="spark://localhost:7077", use_local=False)
        runner = SparkRunner(config)

    ローカル (Docker なし):
        runner = SparkRunner()  # local[*] モード
    """

    def __init__(self, config: SparkConfig | None = None) -> None:
        self.config = config or SparkConfig()
        self._pyspark_available = self._check_pyspark()

    def _check_pyspark(self) -> bool:
        try:
            import pyspark  # noqa: F401
            return True
        except ImportError:
            return False

    def run_dataframe_code(
        self,
        pyspark_code: str,
        var_name: str,
        tables: dict[str, list[dict[str, Any]]],
    ) -> QueryResult:
        """
        PySpark コードを実行して指定変数の DataFrame を QueryResult として返す。

        Args:
            pyspark_code: 実行する PySpark コード (spark 変数を使用)
            var_name:     取得する DataFrame の変数名
            tables:       テーブルデータ {"table.name": [{"col": val, ...}, ...]}
        """
        if not self._pyspark_available:
            raise RuntimeError(
                "pyspark がインストールされていません。\n"
                "Docker 環境 (docker compose up -d) を起動してください。\n"
                "または pip install pyspark でインストールできます。"
            )

        # インプロセスで実行するラッパーコードを生成
        wrapper = self._build_wrapper(pyspark_code, var_name, tables)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(wrapper)
            script_path = f.name

        result_path = script_path + ".result.json"

        try:
            master = "local[*]" if self.config.use_local else self.config.master
            cmd = [
                "spark-submit",
                f"--master={master}",
                f"--driver-memory={self.config.driver_memory}",
                script_path,
                result_path,
            ]
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"spark-submit 失敗\n"
                    f"stdout: {proc.stdout[-2000:]}\n"
                    f"stderr: {proc.stderr[-2000:]}"
                )

            with open(result_path, encoding="utf-8") as f:
                data = json.load(f)

            return QueryResult(
                columns=data["columns"],
                rows=[tuple(r) for r in data["rows"]],
                row_count=len(data["rows"]),
                engine="spark",
            )
        finally:
            Path(script_path).unlink(missing_ok=True)
            Path(result_path).unlink(missing_ok=True)

    def _build_wrapper(
        self,
        user_code: str,
        var_name: str,
        tables: dict[str, list[dict[str, Any]]],
    ) -> str:
        """
        実行ラッパースクリプトを生成する。
        テーブルデータをメモリに展開してから user_code を実行し、
        結果を JSON ファイルに書き出す。
        """
        tables_json = json.dumps(tables)
        return f'''
import sys, json
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("{self.config.app_name}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# テストテーブルを登録
_tables_data = {tables_json!r}
for table_name, rows in _tables_data.items():
    if rows:
        df = spark.createDataFrame(rows)
        df.createOrReplaceTempView(table_name.replace(".", "_"))
        # spark.table("raw.orders") に対応するため catalog にも登録
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {{table_name.split('.')[0]}}")
        except Exception:
            pass

# ユーザーコードを実行
{user_code}

# 結果を収集して JSON に保存
result_df = {var_name}
columns = result_df.columns
rows = [[str(v) if v is not None else None for v in row] for row in result_df.collect()]
output = {{"columns": columns, "rows": rows}}

result_path = sys.argv[1]
with open(result_path, "w", encoding="utf-8") as f:
    json.dump(output, f)

spark.stop()
'''


# ─────────────────────────────────────────────────────────────────────
# インプロセス実行 (テスト用 / Docker なし環境)
# ─────────────────────────────────────────────────────────────────────

class InProcessSparkRunner:
    """
    pyspark がインストールされている場合にインプロセスで実行する
    軽量ランナー (テスト用)。
    spark-submit を使わず SparkSession をインプロセスで起動する。
    """

    def __init__(self) -> None:
        self._session = None

    def _get_session(self):
        if self._session is None:
            try:
                from pyspark.sql import SparkSession
                self._session = (
                    SparkSession.builder
                    .master("local[2]")
                    .appName("bq-converter-test")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate()
                )
                self._session.sparkContext.setLogLevel("ERROR")
            except ImportError:
                raise RuntimeError("pyspark がインストールされていません")
        return self._session

    def run(
        self,
        code: str,
        var_name: str,
        tables: dict[str, list[dict[str, Any]]],
    ) -> QueryResult:
        spark = self._get_session()
        from pyspark.sql import Row

        # テーブルを登録
        for table_name, rows in tables.items():
            if rows:
                df = spark.createDataFrame([Row(**r) for r in rows])
                safe_name = table_name.replace(".", "_")
                df.createOrReplaceTempView(safe_name)

        # コードを実行
        local_vars: dict[str, Any] = {"spark": spark}
        exec(code, local_vars)

        result_df = local_vars[var_name]
        columns = result_df.columns
        rows_data = result_df.collect()
        rows = [tuple(row[c] for c in columns) for row in rows_data]

        return QueryResult(
            columns=list(columns),
            rows=rows,
            row_count=len(rows),
            engine="spark",
        )

    def stop(self) -> None:
        if self._session:
            self._session.stop()
            self._session = None
