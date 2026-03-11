"""
中間表現 (Intermediate Representation)
PySpark の DataFrame 操作を構造化されたノードツリーで表現する。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


# ---------------------------------------------------------------------------
# 式 (Expression) ノード
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Expr:
    """全式ノードの基底クラス"""


@dataclass(frozen=True)
class ColRef(Expr):
    """カラム参照: F.col("x") / df["x"] / "x" """
    name: str
    table_alias: str | None = None


@dataclass(frozen=True)
class Literal(Expr):
    """リテラル値: F.lit(42), F.lit("hello") """
    value: Any
    dtype: str | None = None


@dataclass(frozen=True)
class Alias(Expr):
    """エイリアス: expr.alias("new_name") """
    expr: Expr
    name: str


@dataclass(frozen=True)
class BinaryOp(Expr):
    """二項演算: a + b, a == b, a & b """
    op: str
    left: Expr
    right: Expr


@dataclass(frozen=True)
class UnaryOp(Expr):
    """単項演算: ~cond, -col """
    op: str
    expr: Expr


@dataclass(frozen=True)
class FunctionCall(Expr):
    """関数呼び出し: F.upper(col), F.coalesce(a, b) """
    name: str
    args: tuple[Expr, ...]
    kwargs: tuple[tuple[str, Any], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CaseWhen(Expr):
    """CASE WHEN: F.when(cond, val).when(...).otherwise(default) """
    branches: tuple[tuple[Expr, Expr], ...]
    default: Expr | None = None


@dataclass(frozen=True)
class OrderSpec:
    """ORDER BY の1カラム指定"""
    expr: Expr
    ascending: bool = True
    nulls_last: bool = True


@dataclass(frozen=True)
class WindowFrame:
    frame_type: str  # "ROWS" | "RANGE"
    start: int | str
    end: int | str


@dataclass(frozen=True)
class WindowExpr(Expr):
    """Window 関数: F.row_number().over(Window.partitionBy(...).orderBy(...)) """
    func: Expr
    partition_by: tuple[Expr, ...]
    order_by: tuple[OrderSpec, ...]
    frame: WindowFrame | None = None


@dataclass(frozen=True)
class StarExpr(Expr):
    """SELECT *"""
    table_alias: str | None = None


@dataclass(frozen=True)
class Cast(Expr):
    """型キャスト: F.col("x").cast("string") """
    expr: Expr
    target_type: str


@dataclass(frozen=True)
class IsNull(Expr):
    expr: Expr


@dataclass(frozen=True)
class IsNotNull(Expr):
    expr: Expr


@dataclass(frozen=True)
class InList(Expr):
    """col.isin([...]) """
    expr: Expr
    values: tuple[Expr, ...]


# ---------------------------------------------------------------------------
# 論理プラン (Logical Plan) ノード
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Plan:
    """全プランノードの基底クラス"""


@dataclass(frozen=True)
class SourceTable(Plan):
    """テーブル読み込み"""
    name: str
    alias: str | None = None
    source_type: str = "table"


@dataclass(frozen=True)
class SelectPlan(Plan):
    source: Plan
    columns: tuple[Expr, ...]


@dataclass(frozen=True)
class FilterPlan(Plan):
    source: Plan
    condition: Expr


@dataclass(frozen=True)
class GroupByPlan(Plan):
    source: Plan
    keys: tuple[Expr, ...]
    aggregations: tuple[Expr, ...]
    mode: str = "normal"  # "normal" | "rollup" | "cube"


class JoinType(Enum):
    INNER = auto()
    LEFT = auto()
    RIGHT = auto()
    FULL = auto()
    LEFT_SEMI = auto()
    LEFT_ANTI = auto()
    CROSS = auto()


@dataclass(frozen=True)
class JoinPlan(Plan):
    left: Plan
    right: Plan
    condition: Expr | None
    join_type: JoinType = JoinType.INNER
    using_columns: tuple[str, ...] | None = None


@dataclass(frozen=True)
class OrderByPlan(Plan):
    source: Plan
    order_specs: tuple[OrderSpec, ...]


@dataclass(frozen=True)
class LimitPlan(Plan):
    source: Plan
    n: int


@dataclass(frozen=True)
class DistinctPlan(Plan):
    source: Plan
    subset: tuple[str, ...] | None = None


@dataclass(frozen=True)
class UnionPlan(Plan):
    sources: tuple[Plan, ...]
    distinct: bool = False
    by_name: bool = False


@dataclass(frozen=True)
class WithColumnPlan(Plan):
    source: Plan
    column_name: str
    expr: Expr


@dataclass(frozen=True)
class DropPlan(Plan):
    source: Plan
    columns: tuple[str, ...]


@dataclass(frozen=True)
class RenamePlan(Plan):
    source: Plan
    old_name: str
    new_name: str


@dataclass(frozen=True)
class SubqueryPlan(Plan):
    source: Plan
    alias: str


@dataclass(frozen=True)
class SelectExprPlan(Plan):
    """selectExpr("expr1", "expr2 as alias") — SQL 式を直接 SELECT"""
    source: Plan
    expressions: tuple[str, ...]


@dataclass(frozen=True)
class FillNaPlan(Plan):
    """fillna / na.fill — NULL を指定値で補完"""
    source: Plan
    value: Any
    subset: tuple[str, ...] | None = None


@dataclass(frozen=True)
class DropNaPlan(Plan):
    """dropna / na.drop — NULL 行を除去"""
    source: Plan
    how: str = "any"   # "any" | "all"
    subset: tuple[str, ...] | None = None


@dataclass(frozen=True)
class SamplePlan(Plan):
    """sample — ランダムサンプリング"""
    source: Plan
    fraction: float
    seed: int | None = None


@dataclass(frozen=True)
class ReplacePlan(Plan):
    """replace / na.replace — 値の置換"""
    source: Plan
    to_replace: dict[Any, Any]
    subset: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ToDFPlan(Plan):
    """toDF — 全カラム名の一括変更"""
    source: Plan
    names: tuple[str, ...]


class SetOpType(Enum):
    INTERSECT = auto()
    INTERSECT_ALL = auto()
    EXCEPT = auto()
    EXCEPT_ALL = auto()


@dataclass(frozen=True)
class SetOpPlan(Plan):
    """intersect / intersectAll / subtract / exceptAll"""
    left: Plan
    right: Plan
    op_type: SetOpType


class GroupByMode(Enum):
    NORMAL = auto()
    ROLLUP = auto()
    CUBE = auto()


@dataclass(frozen=True)
class PivotPlan(Plan):
    """pivot — ピボットテーブル"""
    source: Plan
    keys: tuple[Expr, ...]
    pivot_col: str
    pivot_values: tuple[Any, ...]
    aggregations: tuple[Expr, ...]


@dataclass(frozen=True)
class UnpivotPlan(Plan):
    """unpivot / melt — アンピボット"""
    source: Plan
    ids: tuple[str, ...]
    values: tuple[str, ...]
    variable_column_name: str
    value_column_name: str


# ---------------------------------------------------------------------------
# 変換結果 / エラー
# ---------------------------------------------------------------------------

@dataclass
class ConversionResult:
    sql: str
    warnings: list[str] = field(default_factory=list)
    unsupported_patterns: list[str] = field(default_factory=list)
    source_file: str | None = None


class ConversionError(Exception):
    def __init__(self, message: str, node: Any = None):
        super().__init__(message)
        self.node = node


class UnsupportedPatternWarning(UserWarning):
    pass
