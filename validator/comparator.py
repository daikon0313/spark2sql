"""
Comparator: Spark 実行結果 ↔ DuckDB 実行結果 の差分比較

2つの QueryResult を受け取り、行・カラムの一致を検証する。
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from .duckdb_runner import QueryResult


# ─────────────────────────────────────────────────────────────────────
# 比較設定
# ─────────────────────────────────────────────────────────────────────

@dataclass
class CompareConfig:
    """比較挙動の設定"""
    ignore_column_order: bool = True       # カラム順序の違いを無視
    ignore_row_order: bool = True          # 行順序の違いを無視
    float_tolerance: float = 1e-9         # 浮動小数点の許容誤差 (相対)
    null_vs_empty_string: str = "warn"    # "strict" | "warn" | "ignore"
    case_sensitive_strings: bool = True   # 文字列の大文字小文字を区別
    max_diff_rows: int = 10               # 差分レポートに表示する最大行数


# ─────────────────────────────────────────────────────────────────────
# 比較結果型
# ─────────────────────────────────────────────────────────────────────

@dataclass
class CompareResult:
    """2つのクエリ結果の比較結果"""
    is_equal: bool
    row_count_match: bool
    column_match: bool
    diff_rows: list[dict[str, Any]] = field(default_factory=list)   # 差分行 (最大 max_diff_rows)
    missing_in_right: list[dict] = field(default_factory=list)      # 左にあって右にない行
    extra_in_right: list[dict] = field(default_factory=list)        # 右にあって左にない行
    column_diff: list[str] = field(default_factory=list)            # カラムの差分
    warnings: list[str] = field(default_factory=list)
    left_row_count: int = 0
    right_row_count: int = 0

    def summary(self) -> str:
        lines = [
            f"{'✅ MATCH' if self.is_equal else '❌ MISMATCH'}",
            f"  行数 : 左={self.left_row_count} 右={self.right_row_count}"
            f"  {'✅' if self.row_count_match else '❌'}",
            f"  カラム: {'✅' if self.column_match else '❌ ' + str(self.column_diff)}",
        ]
        if self.missing_in_right:
            lines.append(f"  右に存在しない行: {len(self.missing_in_right)} 件")
            for row in self.missing_in_right[:3]:
                lines.append(f"    - {row}")
        if self.extra_in_right:
            lines.append(f"  左に存在しない行: {len(self.extra_in_right)} 件")
            for row in self.extra_in_right[:3]:
                lines.append(f"    + {row}")
        if self.warnings:
            lines.append("  ⚠ 警告:")
            for w in self.warnings:
                lines.append(f"    - {w}")
        return "\n".join(lines)

    def assert_equal(self) -> None:
        """一致しない場合は AssertionError を発生させる"""
        if not self.is_equal:
            raise AssertionError(
                f"結果が一致しません\n{self.summary()}"
            )


# ─────────────────────────────────────────────────────────────────────
# Comparator
# ─────────────────────────────────────────────────────────────────────

class ResultComparator:
    """
    2つの QueryResult を比較する。

    Usage:
        cmp = ResultComparator()
        result = cmp.compare(spark_result, duckdb_result)
        result.assert_equal()
        print(result.summary())
    """

    def __init__(self, config: CompareConfig | None = None) -> None:
        self.config = config or CompareConfig()

    def compare(self, left: QueryResult, right: QueryResult) -> CompareResult:
        """
        2つのクエリ結果を比較する。

        Args:
            left:  基準とする結果 (通常 Spark 実行結果)
            right: 比較対象の結果 (通常 DuckDB 実行結果)
        """
        cfg = self.config
        warnings: list[str] = []

        # ── カラム比較 ─────────────────────────────────────────────
        left_cols = [c.lower() for c in left.columns]
        right_cols = [c.lower() for c in right.columns]

        if cfg.ignore_column_order:
            col_match = set(left_cols) == set(right_cols)
            col_diff = list(set(left_cols).symmetric_difference(set(right_cols)))
        else:
            col_match = left_cols == right_cols
            col_diff = [f"left={left_cols}", f"right={right_cols}"] if not col_match else []

        if not col_match:
            return CompareResult(
                is_equal=False,
                row_count_match=False,
                column_match=False,
                column_diff=col_diff,
                warnings=warnings,
                left_row_count=left.row_count,
                right_row_count=right.row_count,
            )

        # ── 行数比較 ───────────────────────────────────────────────
        row_count_match = left.row_count == right.row_count

        # ── 行データの正規化 ───────────────────────────────────────
        # カラム順序を left に合わせて right を並べ替える
        left_dicts = left.to_dicts()

        if cfg.ignore_column_order:
            right_dicts = _reorder_dicts(right.to_dicts(), left.columns)
        else:
            right_dicts = right.to_dicts()

        # 値を正規化
        left_normalized = [_normalize_row(row, cfg, warnings) for row in left_dicts]
        right_normalized = [_normalize_row(row, cfg, warnings) for row in right_dicts]

        # ── 行の差分計算 ───────────────────────────────────────────
        if cfg.ignore_row_order:
            # 行を文字列化してマルチセット比較
            left_set = _rows_to_multiset(left_normalized)
            right_set = _rows_to_multiset(right_normalized)

            missing = _multiset_diff(left_set, right_set, left_normalized, "left")
            extra   = _multiset_diff(right_set, left_set, right_normalized, "right")
        else:
            missing, extra = _ordered_diff(left_normalized, right_normalized, cfg)

        missing = missing[: cfg.max_diff_rows]
        extra = extra[: cfg.max_diff_rows]

        is_equal = col_match and row_count_match and not missing and not extra

        return CompareResult(
            is_equal=is_equal,
            row_count_match=row_count_match,
            column_match=col_match,
            missing_in_right=missing,
            extra_in_right=extra,
            warnings=warnings,
            left_row_count=left.row_count,
            right_row_count=right.row_count,
        )

    def assert_equal(self, left: QueryResult, right: QueryResult) -> None:
        """一致しない場合は AssertionError を発生させるショートカット"""
        self.compare(left, right).assert_equal()


# ─────────────────────────────────────────────────────────────────────
# ヘルパー関数
# ─────────────────────────────────────────────────────────────────────

def _normalize_row(
    row: dict[str, Any],
    cfg: CompareConfig,
    warnings: list[str],
) -> dict[str, Any]:
    """行の値を正規化する (型・NULL・浮動小数点の統一)"""
    result = {}
    for key, val in row.items():
        normalized_key = key.lower()

        if val is None:
            result[normalized_key] = None
            continue

        # 空文字列 と NULL の扱い
        if val == "" and cfg.null_vs_empty_string == "ignore":
            result[normalized_key] = None
            continue
        if val == "" and cfg.null_vs_empty_string == "warn":
            warnings.append(f"カラム '{key}': 空文字列 を NULL として扱います")
            result[normalized_key] = None
            continue

        # 浮動小数点: NaN → None
        if isinstance(val, float) and math.isnan(val):
            result[normalized_key] = None
            continue

        # 数値の統一 (int → float で比較)
        if isinstance(val, (int, float)):
            result[normalized_key] = float(val)
            continue

        # 文字列
        if isinstance(val, str):
            if not cfg.case_sensitive_strings:
                result[normalized_key] = val.lower()
            else:
                result[normalized_key] = val
            continue

        result[normalized_key] = val

    return result


def _reorder_dicts(
    dicts: list[dict[str, Any]],
    target_columns: list[str],
) -> list[dict[str, Any]]:
    """辞書のキー順序を target_columns に合わせる"""
    target_lower = [c.lower() for c in target_columns]
    result = []
    for d in dicts:
        lower_d = {k.lower(): v for k, v in d.items()}
        result.append({k: lower_d.get(k) for k in target_lower})
    return result


def _row_key(row: dict[str, Any]) -> str:
    """行を比較キー用の文字列に変換"""
    # 浮動小数点は小数点 6 桁で丸める
    parts = []
    for k in sorted(row.keys()):
        v = row[k]
        if isinstance(v, float):
            parts.append(f"{k}={v:.6g}")
        else:
            parts.append(f"{k}={v!r}")
    return "|".join(parts)


def _rows_to_multiset(rows: list[dict]) -> dict[str, int]:
    """行リストをキー→出現回数の辞書に変換"""
    result: dict[str, int] = {}
    for row in rows:
        key = _row_key(row)
        result[key] = result.get(key, 0) + 1
    return result


def _multiset_diff(
    source: dict[str, int],
    target: dict[str, int],
    original_rows: list[dict],
    side: str,
) -> list[dict]:
    """source にあって target にない行を返す"""
    key_to_row = {_row_key(r): r for r in original_rows}
    diff = []
    for key, count in source.items():
        target_count = target.get(key, 0)
        if count > target_count:
            row = key_to_row.get(key, {})
            for _ in range(count - target_count):
                diff.append(row)
    return diff


def _ordered_diff(
    left: list[dict],
    right: list[dict],
    cfg: CompareConfig,
) -> tuple[list[dict], list[dict]]:
    """順序固定で行の差分を計算"""
    missing = []
    extra = []
    n = min(len(left), len(right))
    for i in range(n):
        if not _rows_match(left[i], right[i], cfg):
            missing.append({"index": i, "expected": left[i], "actual": right[i]})
    for i in range(n, len(left)):
        missing.append(left[i])
    for i in range(n, len(right)):
        extra.append(right[i])
    return missing, extra


def _rows_match(
    left: dict[str, Any],
    right: dict[str, Any],
    cfg: CompareConfig,
) -> bool:
    """2行が一致するか判定 (浮動小数点の許容誤差あり)"""
    if left.keys() != right.keys():
        return False
    for key in left:
        lv, rv = left[key], right[key]
        if lv is None and rv is None:
            continue
        if lv is None or rv is None:
            return False
        if isinstance(lv, float) and isinstance(rv, float):
            if lv == 0 and rv == 0:
                continue
            if lv == 0 or rv == 0:
                return abs(lv - rv) < cfg.float_tolerance
            if abs(lv - rv) / max(abs(lv), abs(rv)) > cfg.float_tolerance:
                return False
        elif lv != rv:
            return False
    return True
