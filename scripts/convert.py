#!/usr/bin/env python3
"""
単一ファイル変換 CLI

Usage:
  python scripts/convert.py pipeline.py
  python scripts/convert.py pipeline.py --var result
  python scripts/convert.py pipeline.py --out output.sql
  python scripts/convert.py pipeline.py --validate
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from converter import PySparkToBigQueryTransformer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="PySpark スクリプトを BigQuery SQL に変換する",
    )
    parser.add_argument("file", help="変換対象の PySpark スクリプト (.py)")
    parser.add_argument("--var", help="変換対象の変数名 (省略時は全変数)")
    parser.add_argument("--out", help="出力先ファイル (省略時は stdout)")
    parser.add_argument(
        "--validate", action="store_true",
        help="DuckDB で生成 SQL の構文チェックを実行する",
    )
    args = parser.parse_args(argv)

    path = Path(args.file)
    if not path.exists():
        print(f"エラー: ファイルが見つかりません: {path}", file=sys.stderr)
        return 1

    # 変換
    transformer = PySparkToBigQueryTransformer()
    target_vars = [args.var] if args.var else None
    try:
        results = transformer.convert_file(path, target_vars=target_vars)
    except Exception as e:
        print(f"変換エラー: {e}", file=sys.stderr)
        return 1

    if not results:
        print("エラー: DataFrame 変数が見つかりません", file=sys.stderr)
        return 1

    # SQL 組み立て
    sql_parts: list[str] = []
    has_warnings = False
    has_errors = False

    for var_name, result in results.items():
        if result.sql.startswith("-- ERROR:"):
            has_errors = True

        if result.warnings:
            has_warnings = True
            for w in result.warnings:
                print(f"[WARNING] ({var_name}) {w}", file=sys.stderr)

        header = f"-- Variable: {var_name}"
        sql_parts.append(f"{header}\n{result.sql}")

    sql_output = "\n\n".join(sql_parts) + "\n"

    if has_errors:
        print("エラー: 一部の変数で変換に失敗しました", file=sys.stderr)
        return 1

    # バリデーション
    if args.validate:
        exit_code = _validate_sql(results)
        if exit_code != 0:
            return exit_code

    # 出力
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(sql_output, encoding="utf-8")
        print(f"出力: {out_path}", file=sys.stderr)
    else:
        sys.stdout.write(sql_output)

    if has_warnings:
        return 2
    return 0


def _validate_sql(results: dict) -> int:
    """DuckDB で生成 SQL の構文チェックを実行する"""
    try:
        import duckdb
    except ImportError:
        print("[VALIDATE] duckdb が未インストールです (pip install duckdb)", file=sys.stderr)
        return 1

    errors = []
    for var_name, result in results.items():
        if result.sql.startswith("-- ERROR:"):
            continue
        try:
            conn = duckdb.connect(":memory:")
            # EXPLAIN で構文チェックのみ
            conn.execute(f"EXPLAIN {result.sql}")
            conn.close()
            print(f"[VALIDATE] {var_name}: OK", file=sys.stderr)
        except duckdb.Error as e:
            errors.append((var_name, str(e)))
            print(f"[VALIDATE] {var_name}: FAIL - {e}", file=sys.stderr)

    if errors:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
