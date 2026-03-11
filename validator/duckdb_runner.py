"""
DuckDB Runner

BigQuery SQL (標準 SQL 方言) を DuckDB で実行する検証基盤。
DuckDB は BQ の標準 SQL に近い方言をサポートしており、
ローカル環境での結果検証に使用する。

DuckDB 未インストール時は sqlite3 で限定的な検証を行う。
"""
from __future__ import annotations

import re
import csv
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# DuckDB はオプション依存
try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# ─────────────────────────────────────────────────────────────────────
# 結果型
# ─────────────────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    """クエリ実行結果"""
    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int
    engine: str                          # "duckdb" | "sqlite"
    warnings: list[str] = field(default_factory=list)

    def to_dicts(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, row)) for row in self.rows]

    def to_csv_str(self) -> str:
        import io
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(self.columns)
        w.writerows(self.rows)
        return buf.getvalue()

    def __repr__(self) -> str:
        return (
            f"QueryResult(engine={self.engine!r}, "
            f"rows={self.row_count}, columns={self.columns})"
        )


# ─────────────────────────────────────────────────────────────────────
# テーブル定義
# ─────────────────────────────────────────────────────────────────────

@dataclass
class TableSchema:
    """テーブルのスキーマ定義"""
    name: str                                    # "raw.orders" など
    columns: list[tuple[str, str]]               # [(col_name, bq_type), ...]
    data: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_csv(cls, name: str, csv_path: str | Path) -> "TableSchema":
        """CSV ファイルからスキーマとデータを読み込む"""
        rows = []
        with open(csv_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))

        # カラム型は全て STRING で推論 (簡易実装)
        columns = [(col, "STRING") for col in (rows[0].keys() if rows else [])]
        return cls(name=name, columns=columns, data=rows)

    @classmethod
    def from_json(cls, name: str, json_path: str | Path) -> "TableSchema":
        """JSON ファイルからスキーマとデータを読み込む"""
        with open(json_path, encoding="utf-8") as f:
            rows = json.load(f)
        if not isinstance(rows, list):
            rows = [rows]
        columns = [(col, "STRING") for col in (rows[0].keys() if rows else [])]
        return cls(name=name, columns=columns, data=rows)


# ─────────────────────────────────────────────────────────────────────
# DuckDB Runner
# ─────────────────────────────────────────────────────────────────────

class DuckDBRunner:
    """
    BigQuery SQL を DuckDB で実行する。

    Usage:
        runner = DuckDBRunner()
        runner.register_table(TableSchema(
            name="raw.orders",
            columns=[("order_id","INT64"),("status","STRING"),("amount","FLOAT64")],
            data=[{"order_id":1,"status":"active","amount":100.0}],
        ))
        result = runner.run("SELECT * FROM `raw.orders` WHERE `status` = 'active'")
        print(result.rows)
    """

    def __init__(self, in_memory: bool = True) -> None:
        self._tables: dict[str, TableSchema] = {}
        self._warnings: list[str] = []
        self._engine = "duckdb" if HAS_DUCKDB else "sqlite"
        self._in_memory = in_memory

        if HAS_DUCKDB:
            self._conn = duckdb.connect(":memory:" if in_memory else "local.duckdb")
        else:
            self._conn = sqlite3.connect(":memory:")
            self._warnings.append(
                "duckdb がインストールされていないため sqlite3 で代替実行します。"
                "BQ 固有関数 (DATE_TRUNC, ARRAY_AGG 等) は動作しません。"
            )

    # ── テーブル登録 ──────────────────────────────────────────────────

    def register_table(self, schema: TableSchema) -> None:
        """テーブルをランナーに登録する"""
        self._tables[schema.name] = schema
        safe_name = _safe_table_name(schema.name)

        if HAS_DUCKDB:
            self._register_duckdb(schema, safe_name)
        else:
            self._register_sqlite(schema, safe_name)

    def _register_duckdb(self, schema: TableSchema, safe_name: str) -> None:
        # DuckDB: pandas DataFrame 経由でテーブルを作成
        if HAS_PANDAS and schema.data:
            df = pd.DataFrame(schema.data)
            self._conn.register(safe_name, df)
        elif schema.data:
            # pandas なしの場合は VALUES 句で INSERT
            self._create_and_insert_duckdb(schema, safe_name)
        else:
            # 空テーブル
            col_defs = ", ".join(
                f'"{col}" {_bq_type_to_duckdb(t)}' for col, t in schema.columns
            )
            self._conn.execute(f'CREATE TABLE IF NOT EXISTS "{safe_name}" ({col_defs})')

    def _create_and_insert_duckdb(self, schema: TableSchema, safe_name: str) -> None:
        col_defs = ", ".join(
            f'"{col}" {_bq_type_to_duckdb(t)}' for col, t in schema.columns
        )
        self._conn.execute(f'CREATE TABLE IF NOT EXISTS "{safe_name}" ({col_defs})')
        col_names = [col for col, _ in schema.columns]
        placeholders = ", ".join(["?"] * len(col_names))
        cols_str = ", ".join(f'"{c}"' for c in col_names)
        for row in schema.data:
            values = [row.get(c) for c in col_names]
            self._conn.execute(
                f'INSERT INTO "{safe_name}" ({cols_str}) VALUES ({placeholders})',
                values,
            )

    def _register_sqlite(self, schema: TableSchema, safe_name: str) -> None:
        col_defs = ", ".join(
            f'"{col}" TEXT' for col, _ in schema.columns
        )
        self._conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{safe_name}" ({col_defs})'
        )
        if schema.data:
            col_names = [col for col, _ in schema.columns]
            placeholders = ", ".join(["?"] * len(col_names))
            cols_str = ", ".join(f'"{c}"' for c in col_names)
            for row in schema.data:
                values = [str(row.get(c, "")) if row.get(c) is not None else None
                          for c in col_names]
                self._conn.execute(
                    f'INSERT INTO "{safe_name}" ({cols_str}) VALUES ({placeholders})',
                    values,
                )
            self._conn.commit()

    # ── SQL 実行 ──────────────────────────────────────────────────────

    def run(self, sql: str) -> QueryResult:
        """
        BigQuery SQL を実行して結果を返す。
        バッククォートをダブルクォートに変換し、
        テーブル名をローカル名に置換してから実行する。
        """
        adapted_sql = self._adapt_sql(sql)
        warnings = list(self._warnings)

        try:
            if HAS_DUCKDB:
                result = self._conn.execute(adapted_sql)
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description]
            else:
                cursor = self._conn.cursor()
                cursor.execute(adapted_sql)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

            return QueryResult(
                columns=columns,
                rows=[tuple(r) for r in rows],
                row_count=len(rows),
                engine=self._engine,
                warnings=warnings,
            )

        except Exception as e:
            raise RuntimeError(
                f"SQL 実行エラー ({self._engine})\n"
                f"=== 実行 SQL ===\n{adapted_sql}\n"
                f"=== エラー ===\n{e}"
            ) from e

    def run_file(self, sql_path: str | Path) -> QueryResult:
        """SQL ファイルを読み込んで実行する"""
        sql = Path(sql_path).read_text(encoding="utf-8")
        return self.run(sql)

    # ── SQL アダプター ────────────────────────────────────────────────

    def _adapt_sql(self, sql: str) -> str:
        """BigQuery SQL → DuckDB/SQLite 方言に変換する"""
        adapted = sql

        # 1. バッククォート → ダブルクォート (識別子)
        adapted = adapted.replace("`", '"')

        # 2. テーブル名を safe_name に置換
        #    "raw.orders" → "raw_orders"
        for original_name in self._tables:
            safe = _safe_table_name(original_name)
            # ダブルクォート変換後の形式にマッチ
            quoted_original = '"' + original_name.replace('"', '""') + '"'
            quoted_safe = f'"{safe}"'
            adapted = adapted.replace(quoted_original, quoted_safe)

        # 3. BQ 固有構文を DuckDB/SQLite 互換に変換
        if HAS_DUCKDB:
            adapted = _adapt_bq_to_duckdb(adapted)
        else:
            adapted = _adapt_bq_to_sqlite(adapted)

        return adapted

    # ── ユーティリティ ────────────────────────────────────────────────

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ─────────────────────────────────────────────────────────────────────
# BQ → DuckDB / SQLite 方言変換
# ─────────────────────────────────────────────────────────────────────

def _adapt_bq_to_duckdb(sql: str) -> str:
    """BigQuery 固有構文を DuckDB 方言に変換する"""
    adapted = sql

    # CURRENT_DATE() → CURRENT_DATE (DuckDB は括弧なし)
    adapted = re.sub(r'\bCURRENT_DATE\(\)', 'CURRENT_DATE', adapted)
    adapted = re.sub(r'\bCURRENT_TIMESTAMP\(\)', 'CURRENT_TIMESTAMP', adapted)

    # BQ の EXCEPT → DuckDB は EXCEPT をサポート (同一)
    # SELECT * EXCEPT(col) → DuckDB 1.0+ でサポート

    # FARM_FINGERPRINT → DuckDB 未サポートのため hash に変換
    adapted = re.sub(r'\bFARM_FINGERPRINT\(', 'hash(', adapted)

    # PARSE_DATE / FORMAT_DATE → DuckDB の strptime/strftime
    adapted = re.sub(
        r"PARSE_DATE\('([^']+)',\s*([^)]+)\)",
        lambda m: f"strptime({m.group(2)}, '{m.group(1)}')",
        adapted,
    )
    adapted = re.sub(
        r"FORMAT_DATE\('([^']+)',\s*([^)]+)\)",
        lambda m: f"strftime({m.group(2)}, '{m.group(1)}')",
        adapted,
    )

    # TO_HEX(MD5(x)) → md5(x) (DuckDB は文字列で返す)
    adapted = re.sub(r'\bTO_HEX\(MD5\(', 'md5(', adapted)
    adapted = re.sub(r'\bTO_HEX\(SHA1\(', 'sha1(', adapted)

    # SAFE_DIVIDE(a, b) → (CASE WHEN b = 0 THEN NULL ELSE a / b END)
    adapted = re.sub(
        r'\bSAFE_DIVIDE\(([^,]+),\s*([^)]+)\)',
        r'(CASE WHEN \2 = 0 THEN NULL ELSE \1 / \2 END)',
        adapted,
    )

    # NULLS LAST / NULLS FIRST → DuckDB サポート (そのまま)
    # ARRAY_AGG IGNORE NULLS → DuckDB サポート (そのまま)
    # APPROX_COUNT_DISTINCT → DuckDB は approx_count_distinct (小文字)
    adapted = re.sub(r'\bAPPROX_COUNT_DISTINCT\b', 'approx_count_distinct', adapted)

    return adapted


def _adapt_bq_to_sqlite(sql: str) -> str:
    """BigQuery 固有構文を SQLite 方言に変換する (フォールバック)"""
    adapted = sql

    # WITH 句は SQLite でもサポート (そのまま)

    # BQ 固有関数を SQLite の代替に変換
    adapted = re.sub(r'\bCURRENT_DATE\(\)', 'date("now")', adapted)
    adapted = re.sub(r'\bCURRENT_TIMESTAMP\(\)', 'datetime("now")', adapted)

    # EXTRACT(YEAR FROM x) → strftime('%Y', x)
    _EXTRACT_MAP = {
        "YEAR": "%Y", "MONTH": "%m", "DAY": "%d",
        "HOUR": "%H", "MINUTE": "%M", "SECOND": "%S",
    }
    for part, fmt in _EXTRACT_MAP.items():
        adapted = re.sub(
            rf'\bEXTRACT\({part}\s+FROM\s+([^)]+)\)',
            rf"CAST(strftime('{fmt}', \1) AS INTEGER)",
            adapted,
            flags=re.IGNORECASE,
        )

    # DATE_ADD(d, INTERVAL n DAY) → date(d, '+n days')
    adapted = re.sub(
        r"\bDATE_ADD\(([^,]+),\s*INTERVAL\s+(\d+)\s+DAY\)",
        r"date(\1, '+\2 days')",
        adapted,
        flags=re.IGNORECASE,
    )
    adapted = re.sub(
        r"\bDATE_SUB\(([^,]+),\s*INTERVAL\s+(\d+)\s+DAY\)",
        r"date(\1, '-\2 days')",
        adapted,
        flags=re.IGNORECASE,
    )

    # DATE_DIFF → julianday の差
    adapted = re.sub(
        r"\bDATE_DIFF\(([^,]+),\s*([^,]+),\s*DAY\)",
        r"CAST(julianday(\1) - julianday(\2) AS INTEGER)",
        adapted,
        flags=re.IGNORECASE,
    )

    # COALESCE はそのまま
    # NULLIF はそのまま

    # 未サポートの BQ 固有構文を警告コメントに変換
    unsupported_patterns = [
        r"\bARRAY_AGG\b", r"\bARRAY_LENGTH\b", r"\bSTRUCT\b",
        r"\bPARSE_DATE\b", r"\bFORMAT_DATE\b", r"\bDATE_TRUNC\b",
        r"\bTIMESTAMP_TRUNC\b", r"\bAPPROX_COUNT_DISTINCT\b",
    ]
    for pattern in unsupported_patterns:
        if re.search(pattern, adapted, re.IGNORECASE):
            adapted = f"-- WARNING: この SQL には SQLite 非対応の BQ 関数が含まれます\n{adapted}"
            break

    # SELECT * EXCEPT(col) → SQLite 非サポート → * に変換 (警告)
    adapted = re.sub(r"\bSELECT\s+\*\s+EXCEPT\([^)]+\)", "SELECT *  -- WARNING: EXCEPT not supported", adapted)

    return adapted


# ─────────────────────────────────────────────────────────────────────
# 型変換ヘルパー
# ─────────────────────────────────────────────────────────────────────

def _safe_table_name(name: str) -> str:
    """
    "raw.orders" → "raw_orders"
    識別子として安全な名前に変換する。
    """
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _bq_type_to_duckdb(bq_type: str) -> str:
    """BigQuery 型 → DuckDB 型"""
    mapping = {
        "INT64": "BIGINT",
        "INTEGER": "INTEGER",
        "FLOAT64": "DOUBLE",
        "NUMERIC": "DECIMAL",
        "STRING": "VARCHAR",
        "BOOL": "BOOLEAN",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "BYTES": "BLOB",
        "ARRAY": "VARCHAR[]",
        "JSON": "JSON",
    }
    # NUMERIC(p, s) をそのまま渡す
    if bq_type.upper().startswith("NUMERIC("):
        return bq_type.upper().replace("NUMERIC", "DECIMAL")
    return mapping.get(bq_type.upper(), "VARCHAR")
