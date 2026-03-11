#!/usr/bin/env python3
"""
ディレクトリ一括変換 + レポート出力

Usage:
  python scripts/batch_convert.py                          # base_code/ → converted_code/
  python scripts/batch_convert.py ./pipelines/             # 任意ディレクトリ指定
  python scripts/batch_convert.py ./pipelines/ --out ./sql_output/
  python scripts/batch_convert.py ./pipelines/ --report report.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from converter import PySparkToBigQueryTransformer


@dataclass
class FileResult:
    source_file: str
    status: str = "error"  # "success" | "warning" | "error"
    variables: list[str] = field(default_factory=list)
    sql_path: str | None = None
    warnings: list[str] = field(default_factory=list)
    unsupported_patterns: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class BatchReport:
    total: int = 0
    success: int = 0
    warning: int = 0
    error: int = 0
    auto_conversion_rate: float = 0.0
    elapsed_seconds: float = 0.0
    files: list[FileResult] = field(default_factory=list)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="ディレクトリ配下の PySpark スクリプトを一括で BigQuery SQL に変換する",
    )
    project_root = Path(__file__).resolve().parent.parent
    default_base = str(project_root / "base_code")
    default_out = str(project_root / "converted_code")

    parser.add_argument("dir", nargs="?", default=default_base, help="変換対象ディレクトリ (デフォルト: base_code/)")
    parser.add_argument("--out", default=default_out, help="SQL 出力先ディレクトリ (デフォルト: converted_code/)")
    parser.add_argument("--report", help="JSON レポート出力先")
    parser.add_argument("--recursive", action="store_true", help="サブディレクトリも再帰的に探索")
    args = parser.parse_args(argv)

    src_dir = Path(args.dir)
    if not src_dir.is_dir():
        print(f"エラー: ディレクトリが見つかりません: {src_dir}", file=sys.stderr)
        return 1

    # .py ファイルを収集
    pattern = "**/*.py" if args.recursive else "*.py"
    py_files = sorted(src_dir.glob(pattern))
    if not py_files:
        print(f"エラー: .py ファイルが見つかりません: {src_dir}", file=sys.stderr)
        return 1

    out_dir = Path(args.out) if args.out else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    transformer = PySparkToBigQueryTransformer()
    report = BatchReport(total=len(py_files))
    start = time.monotonic()

    for py_file in py_files:
        file_result = _convert_one(transformer, py_file, out_dir)
        report.files.append(file_result)

        if file_result.status == "success":
            report.success += 1
        elif file_result.status == "warning":
            report.warning += 1
        else:
            report.error += 1

    report.elapsed_seconds = round(time.monotonic() - start, 3)
    report.auto_conversion_rate = round(report.success / report.total, 4) if report.total else 0.0

    # サマリー出力
    _print_summary(report)

    # summary.md 出力 (converted_code/ と同じディレクトリ)
    _write_summary_md(report, out_dir)

    # JSON レポート
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(asdict(report), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"\nレポート出力: {report_path}", file=sys.stderr)

    if report.error > 0:
        return 1
    if report.warning > 0:
        return 2
    return 0


def _convert_one(
    transformer: PySparkToBigQueryTransformer,
    py_file: Path,
    out_dir: Path | None,
) -> FileResult:
    file_result = FileResult(source_file=str(py_file))

    try:
        results = transformer.convert_file(py_file)
    except Exception as e:
        file_result.status = "error"
        file_result.error = str(e)
        print(f"  ERROR  {py_file}: {e}", file=sys.stderr)
        return file_result

    if not results:
        file_result.status = "error"
        file_result.error = "DataFrame 変数が見つかりません"
        print(f"  ERROR  {py_file}: DataFrame 変数が見つかりません", file=sys.stderr)
        return file_result

    sql_parts: list[str] = []
    all_warnings: list[str] = []
    all_unsupported: list[str] = []
    has_error = False

    for var_name, result in results.items():
        file_result.variables.append(var_name)

        if result.sql.startswith("-- ERROR:"):
            has_error = True
            file_result.error = result.sql

        all_warnings.extend(result.warnings)
        all_unsupported.extend(result.unsupported_patterns)
        sql_parts.append(f"-- Variable: {var_name}\n{result.sql}")

    file_result.warnings = all_warnings
    file_result.unsupported_patterns = all_unsupported

    if has_error:
        file_result.status = "error"
        print(f"  ERROR  {py_file}", file=sys.stderr)
    elif all_warnings or all_unsupported:
        file_result.status = "warning"
        print(f"  WARN   {py_file} ({len(all_warnings)} warnings)", file=sys.stderr)
    else:
        file_result.status = "success"
        print(f"  OK     {py_file}", file=sys.stderr)

    # SQL ファイル出力
    if out_dir:
        sql_file = out_dir / py_file.with_suffix(".sql").name
        sql_output = "\n\n".join(sql_parts) + "\n"
        sql_file.write_text(sql_output, encoding="utf-8")
        file_result.sql_path = str(sql_file)

    return file_result


def _print_summary(report: BatchReport) -> None:
    print("\n" + "=" * 50, file=sys.stderr)
    print("変換結果サマリー", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print(f"  合計:     {report.total} ファイル", file=sys.stderr)
    print(f"  成功:     {report.success}", file=sys.stderr)
    print(f"  警告あり: {report.warning}", file=sys.stderr)
    print(f"  エラー:   {report.error}", file=sys.stderr)
    print(f"  自動変換率: {report.auto_conversion_rate:.1%}", file=sys.stderr)
    print(f"  処理時間:   {report.elapsed_seconds:.3f}s", file=sys.stderr)
    print("=" * 50, file=sys.stderr)

    # OK 以外のファイルの詳細を出力
    non_ok = [f for f in report.files if f.status != "success"]
    if non_ok:
        print("\n--- 要確認ファイル ---", file=sys.stderr)
        for f in non_ok:
            sql_name = Path(f.sql_path).name if f.sql_path else "(未出力)"
            print(f"\n  [{f.status.upper()}] {sql_name}", file=sys.stderr)
            if f.error:
                print(f"    エラー: {f.error}", file=sys.stderr)
            for w in f.warnings:
                print(f"    警告: {w}", file=sys.stderr)
        print("", file=sys.stderr)


def _write_summary_md(report: BatchReport, out_dir: Path | None) -> None:
    """converted_code/summary.md にサマリーを出力する"""
    if out_dir is None:
        return

    lines: list[str] = []
    lines.append("# 変換結果サマリー\n")
    lines.append(f"| 項目 | 値 |")
    lines.append(f"|------|-----|")
    lines.append(f"| 合計 | {report.total} ファイル |")
    lines.append(f"| 成功 | {report.success} |")
    lines.append(f"| 警告あり | {report.warning} |")
    lines.append(f"| エラー | {report.error} |")
    lines.append(f"| 自動変換率 | {report.auto_conversion_rate:.1%} |")
    lines.append(f"| 処理時間 | {report.elapsed_seconds:.3f}s |")
    lines.append("")

    # ファイル一覧
    lines.append("## ファイル一覧\n")
    lines.append("| ファイル | ステータス | 詳細 |")
    lines.append("|---------|----------|------|")
    for f in report.files:
        sql_name = Path(f.sql_path).name if f.sql_path else "-"
        if f.status == "success":
            lines.append(f"| {sql_name} | OK | - |")
        elif f.status == "warning":
            lines.append(f"| {sql_name} | WARNING | {len(f.warnings)} 件の警告 |")
        else:
            lines.append(f"| {sql_name} | ERROR | {f.error or '不明'} |")
    lines.append("")

    # 要確認ファイルの詳細
    non_ok = [f for f in report.files if f.status != "success"]
    if non_ok:
        lines.append("## 要確認ファイル\n")
        for f in non_ok:
            sql_name = Path(f.sql_path).name if f.sql_path else "(未出力)"
            lines.append(f"### {sql_name}\n")
            if f.error:
                lines.append(f"- **エラー**: {f.error}")
            for w in f.warnings:
                lines.append(f"- {w}")
            lines.append("")

    summary_path = out_dir / "summary.md"
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"サマリー出力: {summary_path}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
