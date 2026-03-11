"""
Transformer: PySpark → BigQuery SQL の統合エントリポイント
"""
from __future__ import annotations
import re
from pathlib import Path

from .ir import ConversionResult
from .parser import PySparkParser
from .emitter import SQLEmitter


class PySparkToBigQueryTransformer:
    def __init__(self, spark_var: str = "spark") -> None:
        self._parser = PySparkParser(spark_var=spark_var)
        self._emitter = SQLEmitter()

    def convert_file(self, path: str | Path, target_vars: list[str] | None = None) -> dict[str, ConversionResult]:
        source = Path(path).read_text(encoding="utf-8")
        return self.convert_code(source, source_file=str(path), target_vars=target_vars)

    def convert_code(self, source: str, source_file: str | None = None, target_vars: list[str] | None = None) -> dict[str, ConversionResult]:
        plans = self._parser.parse_code(source)
        parser_warnings = self._parser.warnings
        results: dict[str, ConversionResult] = {}

        for var_name, plan in plans.items():
            if target_vars and var_name not in target_vars:
                continue
            try:
                sql, emit_warnings = self._emitter.emit(plan)
                all_warnings = parser_warnings + emit_warnings
                unsupported = [ln.strip() for ln in sql.splitlines() if "/* WARNING:" in ln]
                results[var_name] = ConversionResult(
                    sql=_format_sql(sql),
                    warnings=all_warnings,
                    unsupported_patterns=unsupported,
                    source_file=source_file,
                )
            except Exception as e:
                import traceback
                results[var_name] = ConversionResult(
                    sql=f"-- ERROR: {e}",
                    warnings=parser_warnings + [f"変換エラー ({var_name}): {e}\n{traceback.format_exc()}"],
                    source_file=source_file,
                )
        return results

    def convert_single(self, source: str, var_name: str | None = None) -> ConversionResult:
        results = self.convert_code(source)
        if not results:
            return ConversionResult(sql="-- No DataFrame found", warnings=self._parser.warnings)
        if var_name and var_name in results:
            return results[var_name]
        return list(results.values())[-1]


def _format_sql(sql: str) -> str:
    sql = re.sub(r"\n{3,}", "\n\n", sql)
    return "\n".join(ln.rstrip() for ln in sql.splitlines()).strip()
