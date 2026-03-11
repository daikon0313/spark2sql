"""
PySpark コードの AST パーサー

Python の ast モジュールを使って PySpark スクリプトを解析し、
DataFrame の操作チェーンを IR (中間表現) のプランツリーへ変換する。

対応パターン:
  - spark.table("name")
  - spark.read.parquet / csv / json
  - df.select(...)
  - df.filter(...) / df.where(...)
  - df.groupBy(...).agg(...)
  - df.join(other, on=..., how=...)
  - df.orderBy(...) / df.sort(...)
  - df.limit(n)
  - df.distinct() / df.dropDuplicates(...)
  - df.union(other) / df.unionByName(other)
  - df.withColumn("name", expr)
  - df.drop("col1", ...)
  - df.withColumnRenamed("old", "new")
  - F.col / F.lit / F.when / F.coalesce / etc.
  - Window.partitionBy(...).orderBy(...)
"""
from __future__ import annotations

import ast
import textwrap
import warnings
from pathlib import Path
from typing import Any

from .ir import (
    Alias, BinaryOp, Cast, CaseWhen, ColRef, ConversionError,
    DistinctPlan, DropNaPlan, DropPlan, Expr, FillNaPlan, FilterPlan,
    FunctionCall, GroupByPlan, InList, IsNotNull, IsNull, JoinPlan,
    JoinType, LimitPlan, Literal, OrderByPlan, OrderSpec, PivotPlan,
    Plan, RenamePlan, ReplacePlan, SamplePlan, SelectExprPlan,
    SelectPlan, SetOpPlan, SetOpType, SourceTable, StarExpr,
    SubqueryPlan, ToDFPlan, UnaryOp, UnionPlan, UnpivotPlan,
    UnsupportedPatternWarning, WindowExpr, WindowFrame, WithColumnPlan,
)

# ---------------------------------------------------------------------------
# 内部ヘルパープラン (_PendingGroupBy)
# ---------------------------------------------------------------------------

from dataclasses import dataclass as _dc

@_dc(frozen=True)
class _PendingGroupBy(Plan):
    """groupBy(...) だけで .agg() がまだ来ていない状態を保持する"""
    source: Plan
    keys: tuple
    mode: str = "normal"  # "normal" | "rollup" | "cube"


@_dc(frozen=True)
class _PendingPivot(Plan):
    """groupBy(...).pivot(...) で .agg() がまだ来ていない状態を保持する"""
    source: Plan
    keys: tuple
    pivot_col: str
    pivot_values: tuple


# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

# PySpark の how 文字列 → JoinType マッピング
_JOIN_TYPE_MAP: dict[str, JoinType] = {
    "inner":      JoinType.INNER,
    "left":       JoinType.LEFT,
    "left_outer": JoinType.LEFT,
    "right":      JoinType.RIGHT,
    "right_outer":JoinType.RIGHT,
    "full":       JoinType.FULL,
    "outer":      JoinType.FULL,
    "full_outer": JoinType.FULL,
    "leftsemi":   JoinType.LEFT_SEMI,
    "left_semi":  JoinType.LEFT_SEMI,
    "leftanti":   JoinType.LEFT_ANTI,
    "left_anti":  JoinType.LEFT_ANTI,
    "cross":      JoinType.CROSS,
}

# DataFrame メソッド名 → 変換ハンドラ名のマッピング
_DF_METHODS = {
    "select", "selectExpr", "filter", "where",
    "groupBy", "groupby", "rollup", "cube",
    "join", "crossJoin", "orderBy", "sort", "limit", "distinct",
    "dropDuplicates", "drop_duplicates", "union", "unionByName",
    "unionAll", "withColumn", "drop", "withColumnRenamed",
    "alias", "agg",
    "intersect", "intersectAll", "subtract", "exceptAll",
    "fillna", "dropna", "toDF", "sample", "replace",
    "unpivot", "melt",
    "crosstab", "describe", "summary",
}

# na.fill / na.drop / na.replace のメソッド名
_NA_METHODS = {"fill", "drop", "replace"}

# ---------------------------------------------------------------------------
# メインパーサー
# ---------------------------------------------------------------------------

class PySparkParser:
    """
    PySpark スクリプトを解析して IR プランツリーを返す。

    Usage:
        parser = PySparkParser()
        plan = parser.parse_file("pipeline.py")
        # or
        plan = parser.parse_code(source_code, variable="result_df")
    """

    def __init__(self, spark_var: str = "spark") -> None:
        """
        Args:
            spark_var: SparkSession の変数名（デフォルト "spark"）
        """
        self.spark_var = spark_var
        self._df_aliases: dict[str, Plan] = {}   # 変数名 → Plan のマッピング
        self._window_specs: dict[str, dict] = {}  # 変数名 → window spec dict
        self._warnings: list[str] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_file(self, path: str | Path) -> dict[str, Plan]:
        """
        ファイルを解析して {変数名: Plan} の辞書を返す。
        最終的な DataFrame 変数が複数ある場合もすべて返す。
        """
        source = Path(path).read_text(encoding="utf-8")
        return self.parse_code(source)

    def parse_code(self, source: str) -> dict[str, Plan]:
        """
        ソースコード文字列を解析して {変数名: Plan} の辞書を返す。
        """
        self._df_aliases = {}
        self._window_specs = {}
        self._warnings = []
        tree = ast.parse(textwrap.dedent(source))
        self._visit_module(tree)
        return dict(self._df_aliases)

    @property
    def warnings(self) -> list[str]:
        return list(self._warnings)

    # ------------------------------------------------------------------
    # モジュールレベルの処理
    # ------------------------------------------------------------------

    def _visit_module(self, tree: ast.Module) -> None:
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                self._visit_assign(node)

    def _visit_assign(self, node: ast.Assign) -> None:
        """代入文を処理: df = spark.table(...) など"""
        if len(node.targets) != 1:
            return
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            return
        var_name = target.id

        try:
            # Window spec の検出 (Window.partitionBy(...) など)
            if self._is_window_spec(node.value):
                spec = self._parse_window_spec(node.value)
                self._window_specs[var_name] = spec
                return

            plan = self._parse_plan_expr(node.value)
            if plan is not None:
                self._df_aliases[var_name] = plan
        except ConversionError as e:
            self._warnings.append(f"[{var_name}] 変換スキップ: {e}")
        except Exception as e:
            self._warnings.append(f"[{var_name}] 解析エラー: {e}")

    def _is_window_spec(self, node: ast.expr) -> bool:
        """Window.partitionBy / Window.orderBy / Window.rowsBetween で始まるチェーンか判定"""
        if not isinstance(node, ast.Call):
            return False
        func = node.func
        if not isinstance(func, ast.Attribute):
            return False
        # チェーンを遡って Window. で始まるか確認
        obj = func.value
        while isinstance(obj, ast.Call):
            if not isinstance(obj.func, ast.Attribute):
                break
            obj = obj.func.value
        return (
            isinstance(obj, ast.Name) and obj.id == "Window"
            or isinstance(obj, ast.Attribute) and obj.attr == "Window"
        )

    # ------------------------------------------------------------------
    # プランの解析
    # ------------------------------------------------------------------

    def _parse_plan_expr(self, node: ast.expr) -> Plan | None:
        """
        任意の AST 式を Plan へ変換する。
        DataFrame を返さない式は None を返す。
        """
        if isinstance(node, ast.Call):
            return self._parse_call_as_plan(node)
        if isinstance(node, ast.Name):
            # 既知の DataFrame 変数を参照している
            return self._df_aliases.get(node.id)
        return None

    def _parse_call_as_plan(self, node: ast.Call) -> Plan | None:
        """ast.Call を Plan へ変換"""
        if not isinstance(node.func, ast.Attribute):
            return None

        method = node.func.attr
        obj = node.func.value

        # ── spark.table("name") ──────────────────────────────────────
        if self._is_spark_attr(obj, "table"):
            table_name = self._parse_string_arg(node, 0, "table")
            return SourceTable(name=table_name, source_type="table")

        # ── spark.read.parquet / csv / json ─────────────────────────
        if method in ("parquet", "csv", "json", "orc") and self._is_spark_read(obj):
            path_arg = self._parse_string_arg(node, 0, method)
            return SourceTable(name=path_arg, source_type=method)

        # ── spark.read.format("xxx").load("path") ──────────────────
        if method == "load" and isinstance(obj, ast.Call):
            if isinstance(obj.func, ast.Attribute) and obj.func.attr == "format":
                if self._is_spark_read(obj.func.value):
                    fmt = self._eval_string(obj.args[0]) if obj.args else "parquet"
                    path_arg = self._parse_string_arg(node, 0, "load") if node.args else fmt
                    return SourceTable(name=path_arg, source_type=fmt)

        # ── df.na.fill / df.na.drop / df.na.replace ───────────────
        if method in _NA_METHODS and isinstance(obj, ast.Attribute) and obj.attr == "na":
            source_plan = self._parse_plan_expr(obj.value)
            if source_plan is None and isinstance(obj.value, ast.Name):
                source_plan = SourceTable(name=obj.value.id, source_type="reference")
            if source_plan is None:
                return None
            if method == "fill":
                return self._parse_fillna(source_plan, node)
            if method == "drop":
                return self._parse_dropna(source_plan, node)
            if method == "replace":
                return self._parse_replace(source_plan, node)

        # ── pivot (groupBy(...).pivot(...).agg(...)) ───────────────
        if method == "pivot":
            source_plan = self._parse_plan_expr(obj)
            if isinstance(source_plan, _PendingGroupBy):
                pivot_col = self._eval_string(node.args[0])
                pivot_values = ()
                if len(node.args) > 1 and isinstance(node.args[1], ast.List):
                    pivot_values = tuple(self._eval_literal(e) for e in node.args[1].elts)
                return _PendingPivot(
                    source=source_plan.source,
                    keys=source_plan.keys,
                    pivot_col=pivot_col,
                    pivot_values=pivot_values,
                )

        # ── DataFrame メソッドチェーン ─────────────────────────────
        if method not in _DF_METHODS:
            return None

        source_plan = self._parse_plan_expr(obj)
        if source_plan is None:
            # 変数が未登録なら名前でプレースホルダーを作成
            if isinstance(obj, ast.Name):
                source_plan = SourceTable(name=obj.id, source_type="reference")
            else:
                return None

        return self._parse_df_method(method, source_plan, node)

    def _parse_df_method(
        self, method: str, source: Plan, call: ast.Call
    ) -> Plan | None:
        """DataFrame のメソッド呼び出しを Plan へ変換"""
        args = call.args
        kwargs = {kw.arg: kw.value for kw in call.keywords}

        # select
        if method == "select":
            cols = tuple(self._parse_col_expr(a) for a in args)
            return SelectPlan(source=source, columns=cols)

        # selectExpr
        if method == "selectExpr":
            exprs = tuple(self._eval_string(a) for a in args)
            return SelectExprPlan(source=source, expressions=exprs)

        # filter / where
        if method in ("filter", "where"):
            cond = self._parse_expr(args[0]) if args else self._parse_expr(kwargs["condition"])
            return FilterPlan(source=source, condition=cond)

        # groupBy / groupby → .agg() は呼び出し元でチェーン
        if method in ("groupBy", "groupby"):
            keys = tuple(self._parse_col_expr(a) for a in args)
            return _PendingGroupBy(source=source, keys=keys)

        # rollup
        if method == "rollup":
            keys = tuple(self._parse_col_expr(a) for a in args)
            return _PendingGroupBy(source=source, keys=keys, mode="rollup")

        # cube
        if method == "cube":
            keys = tuple(self._parse_col_expr(a) for a in args)
            return _PendingGroupBy(source=source, keys=keys, mode="cube")

        # agg (groupBy / rollup / cube / pivot の後)
        if method == "agg":
            if isinstance(source, _PendingPivot):
                aggs = tuple(self._parse_expr(a) for a in args)
                return PivotPlan(
                    source=source.source,
                    keys=source.keys,
                    pivot_col=source.pivot_col,
                    pivot_values=source.pivot_values,
                    aggregations=aggs,
                )
            if isinstance(source, _PendingGroupBy):
                aggs = tuple(self._parse_expr(a) for a in args)
                return GroupByPlan(
                    source=source.source,
                    keys=source.keys,
                    aggregations=aggs,
                    mode=source.mode,
                )
            # agg 単体 (groupBy なし) は全行集計
            aggs = tuple(self._parse_expr(a) for a in args)
            return GroupByPlan(source=source, keys=(), aggregations=aggs)

        # crossJoin
        if method == "crossJoin":
            right_plan = self._parse_plan_expr(args[0])
            if right_plan is None and isinstance(args[0], ast.Name):
                right_plan = SourceTable(name=args[0].id, source_type="reference")
            return JoinPlan(
                left=source, right=right_plan,
                condition=None, join_type=JoinType.CROSS,
            )

        # join
        if method == "join":
            right_plan = self._parse_plan_expr(args[0])
            if right_plan is None and isinstance(args[0], ast.Name):
                right_plan = SourceTable(name=args[0].id, source_type="reference")

            on_node = kwargs.get("on") or (args[1] if len(args) > 1 else None)
            how_node = kwargs.get("how") or (args[2] if len(args) > 2 else None)
            how_str = self._eval_string(how_node) if how_node else "inner"
            join_type = _JOIN_TYPE_MAP.get(how_str.lower(), JoinType.INNER)

            # on= がリストの場合 (同名カラム JOIN)
            if on_node and isinstance(on_node, ast.List):
                using_cols = tuple(
                    self._eval_string(e) for e in on_node.elts
                )
                return JoinPlan(
                    left=source,
                    right=right_plan,
                    condition=None,
                    join_type=join_type,
                    using_columns=using_cols,
                )
            condition = self._parse_expr(on_node) if on_node else None
            return JoinPlan(
                left=source, right=right_plan,
                condition=condition, join_type=join_type,
            )

        # orderBy / sort
        if method in ("orderBy", "sort"):
            specs = tuple(self._parse_order_spec(a) for a in args)
            return OrderByPlan(source=source, order_specs=specs)

        # limit
        if method == "limit":
            n = self._eval_int(args[0])
            return LimitPlan(source=source, n=n)

        # distinct
        if method == "distinct":
            return DistinctPlan(source=source)

        # dropDuplicates / drop_duplicates
        if method in ("dropDuplicates", "drop_duplicates"):
            subset: tuple[str, ...] | None = None
            if args and isinstance(args[0], ast.List):
                subset = tuple(self._eval_string(e) for e in args[0].elts)
            return DistinctPlan(source=source, subset=subset)

        # union / unionAll
        if method in ("union", "unionAll"):
            other = self._parse_plan_expr(args[0])
            if other is None:
                return None
            if isinstance(source, UnionPlan) and not source.distinct:
                return UnionPlan(sources=source.sources + (other,), distinct=False)
            return UnionPlan(sources=(source, other), distinct=False)

        # unionByName
        if method == "unionByName":
            other = self._parse_plan_expr(args[0])
            if other is None:
                return None
            return UnionPlan(sources=(source, other), distinct=False, by_name=True)

        # withColumn
        if method == "withColumn":
            col_name = self._eval_string(args[0])
            expr = self._parse_expr(args[1])
            return WithColumnPlan(source=source, column_name=col_name, expr=expr)

        # drop
        if method == "drop":
            cols = tuple(self._eval_string(a) for a in args)
            return DropPlan(source=source, columns=cols)

        # withColumnRenamed
        if method == "withColumnRenamed":
            old = self._eval_string(args[0])
            new = self._eval_string(args[1])
            return RenamePlan(source=source, old_name=old, new_name=new)

        # alias (df.alias("t")) → SubqueryPlan
        if method == "alias":
            alias_name = self._eval_string(args[0])
            return SubqueryPlan(source=source, alias=alias_name)

        # intersect
        if method == "intersect":
            other = self._parse_plan_expr(args[0])
            if other is None:
                return None
            return SetOpPlan(left=source, right=other, op_type=SetOpType.INTERSECT)

        # intersectAll
        if method == "intersectAll":
            other = self._parse_plan_expr(args[0])
            if other is None:
                return None
            return SetOpPlan(left=source, right=other, op_type=SetOpType.INTERSECT_ALL)

        # subtract
        if method == "subtract":
            other = self._parse_plan_expr(args[0])
            if other is None:
                return None
            return SetOpPlan(left=source, right=other, op_type=SetOpType.EXCEPT)

        # exceptAll
        if method == "exceptAll":
            other = self._parse_plan_expr(args[0])
            if other is None:
                return None
            return SetOpPlan(left=source, right=other, op_type=SetOpType.EXCEPT_ALL)

        # fillna
        if method == "fillna":
            return self._parse_fillna(source, call)

        # dropna
        if method == "dropna":
            return self._parse_dropna(source, call)

        # replace
        if method == "replace":
            return self._parse_replace(source, call)

        # toDF
        if method == "toDF":
            names = tuple(self._eval_string(a) for a in args)
            return ToDFPlan(source=source, names=names)

        # sample
        if method == "sample":
            fraction = 0.1
            seed = None
            if args:
                # sample(withReplacement, fraction, seed) or sample(fraction)
                if isinstance(args[0], ast.Constant) and isinstance(args[0].value, bool):
                    # sample(False, 0.1) or sample(False, 0.1, 42)
                    fraction = float(self._eval_literal(args[1])) if len(args) > 1 else 0.1
                    seed = self._eval_int(args[2]) if len(args) > 2 else None
                else:
                    fraction = float(self._eval_literal(args[0]))
                    seed = self._eval_int(args[1]) if len(args) > 1 else None
            if "fraction" in kwargs:
                fraction = float(self._eval_literal(kwargs["fraction"]))
            if "seed" in kwargs:
                seed = self._eval_int(kwargs["seed"])
            return SamplePlan(source=source, fraction=fraction, seed=seed)

        # unpivot / melt
        if method in ("unpivot", "melt"):
            ids_node = args[0] if args else kwargs.get("ids")
            values_node = args[1] if len(args) > 1 else kwargs.get("values")
            var_col = self._eval_string(args[2]) if len(args) > 2 else kwargs.get("variableColumnName", "variable")
            val_col = self._eval_string(args[3]) if len(args) > 3 else kwargs.get("valueColumnName", "value")
            if isinstance(var_col, ast.expr):
                var_col = self._eval_string(var_col)
            if isinstance(val_col, ast.expr):
                val_col = self._eval_string(val_col)
            ids = tuple(self._eval_string(e) for e in ids_node.elts) if isinstance(ids_node, ast.List) else ()
            values = tuple(self._eval_string(e) for e in values_node.elts) if isinstance(values_node, ast.List) else ()
            return UnpivotPlan(
                source=source, ids=ids, values=values,
                variable_column_name=var_col, value_column_name=val_col,
            )

        # crosstab / describe / summary → WARNING
        if method in ("crosstab", "describe", "summary"):
            self._warnings.append(
                f"{method}() は BigQuery SQL に直接変換できません。手動で書き換えてください"
            )
            return source

        return None

    # ------------------------------------------------------------------
    # fillna / dropna / replace ヘルパー
    # ------------------------------------------------------------------

    def _parse_fillna(self, source: Plan, call: ast.Call) -> Plan:
        args = call.args
        kwargs_map = {kw.arg: kw.value for kw in call.keywords}
        value = self._eval_literal(kwargs_map.get("value", args[0] if args else None))
        subset: tuple[str, ...] | None = None
        subset_node = kwargs_map.get("subset", args[1] if len(args) > 1 else None)
        if subset_node and isinstance(subset_node, ast.List):
            subset = tuple(self._eval_string(e) for e in subset_node.elts)
        # value が dict の場合: fillna({"col1": 0, "col2": "x"})
        if args and isinstance(args[0], ast.Dict):
            value = {}
            for k, v in zip(args[0].keys, args[0].values):
                value[self._eval_string(k)] = self._eval_literal(v)
        return FillNaPlan(source=source, value=value, subset=subset)

    def _parse_dropna(self, source: Plan, call: ast.Call) -> Plan:
        args = call.args
        kwargs_map = {kw.arg: kw.value for kw in call.keywords}
        how = "any"
        if "how" in kwargs_map:
            how = self._eval_string(kwargs_map["how"])
        elif args and isinstance(args[0], ast.Constant) and isinstance(args[0].value, str):
            how = args[0].value
        subset: tuple[str, ...] | None = None
        subset_node = kwargs_map.get("subset")
        if subset_node and isinstance(subset_node, ast.List):
            subset = tuple(self._eval_string(e) for e in subset_node.elts)
        return DropNaPlan(source=source, how=how, subset=subset)

    def _parse_replace(self, source: Plan, call: ast.Call) -> Plan:
        args = call.args
        kwargs_map = {kw.arg: kw.value for kw in call.keywords}
        to_replace: dict = {}
        if args and isinstance(args[0], ast.Dict):
            for k, v in zip(args[0].keys, args[0].values):
                to_replace[self._eval_literal(k)] = self._eval_literal(v)
        elif len(args) >= 2:
            old_val = self._eval_literal(args[0])
            new_val = self._eval_literal(args[1])
            if isinstance(old_val, list) and isinstance(new_val, list):
                to_replace = dict(zip(old_val, new_val))
            else:
                to_replace = {old_val: new_val}
        subset: tuple[str, ...] | None = None
        subset_node = kwargs_map.get("subset")
        if subset_node and isinstance(subset_node, ast.List):
            subset = tuple(self._eval_string(e) for e in subset_node.elts)
        return ReplacePlan(source=source, to_replace=to_replace, subset=subset)

    # ------------------------------------------------------------------
    # 式の解析
    # ------------------------------------------------------------------

    def _parse_expr(self, node: ast.expr | None) -> Expr:
        """任意の AST 式を IR の Expr へ変換"""
        if node is None:
            raise ConversionError("式が None です")

        # ── リテラル ─────────────────────────────────────────────────
        if isinstance(node, ast.Constant):
            return Literal(value=node.value)

        # ── 変数名（単純なカラム名文字列として扱う）─────────────────
        if isinstance(node, ast.Name):
            if node.id in ("True", "False", "None"):
                return Literal(value={"True": True, "False": False, "None": None}[node.id])
            # F モジュール参照などは後続で処理
            return ColRef(name=node.id)

        # ── 文字列のカラム参照: "col_name" ───────────────────────────
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return ColRef(name=node.value)

        # ── 属性アクセス: df["col"] や df.col ────────────────────────
        if isinstance(node, ast.Subscript):
            return self._parse_subscript(node)

        # ── 関数/メソッド呼び出し ─────────────────────────────────────
        if isinstance(node, ast.Call):
            return self._parse_call_as_expr(node)

        # ── 二項演算 ─────────────────────────────────────────────────
        if isinstance(node, ast.BinOp):
            return BinaryOp(
                op=_BINOP_MAP.get(type(node.op).__name__, type(node.op).__name__),
                left=self._parse_expr(node.left),
                right=self._parse_expr(node.right),
            )

        # ── 比較演算 ─────────────────────────────────────────────────
        if isinstance(node, ast.Compare):
            return self._parse_compare(node)

        # ── ブール演算 ─────────────────────────────────────────────────
        if isinstance(node, ast.BoolOp):
            return self._parse_boolop(node)

        # ── 単項演算 ─────────────────────────────────────────────────
        if isinstance(node, ast.UnaryOp):
            op = _UNARYOP_MAP.get(type(node.op).__name__, type(node.op).__name__)
            return UnaryOp(op=op, expr=self._parse_expr(node.operand))

        # ── リスト: F.col("x").isin([1, 2, 3]) ───────────────────────
        if isinstance(node, ast.List):
            # リストをそのまま返すには InList が使えないので Literal のタプルで返す
            return Literal(value=[self._parse_expr(e) for e in node.elts])

        raise ConversionError(f"未対応の式ノード: {ast.dump(node)}")


    def _parse_col_expr(self, node: ast.expr) -> "Expr":
        """
        SELECT や GROUP BY のカラム指定に使う式パーサー。
        文字列定数はカラム名 (ColRef) として解釈する。
        """
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return ColRef(name=node.value)
        return self._parse_expr(node)

    def _parse_subscript(self, node: ast.Subscript) -> Expr:
        """df["col"] → ColRef("col")"""
        if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
            return ColRef(name=node.slice.value)
        raise ConversionError(f"未対応のSubscript: {ast.dump(node)}")

    def _parse_call_as_expr(self, node: ast.Call) -> Expr:
        """関数/メソッド呼び出しを Expr へ変換"""
        func = node.func

        # ── F.col("name") ─────────────────────────────────────────────
        if self._is_f_func(func, "col"):
            name = self._eval_string(node.args[0])
            return ColRef(name=name)

        # ── F.lit(value) ──────────────────────────────────────────────
        if self._is_f_func(func, "lit"):
            return Literal(value=self._eval_literal(node.args[0]))

        # ── F.when(cond, val).when(...).otherwise(default) ────────────
        if self._is_f_func(func, "when"):
            return self._parse_when_chain(node)

        # ── メソッドチェーン上の .when / .otherwise ───────────────────
        if isinstance(func, ast.Attribute) and func.attr == "when":
            return self._parse_when_continuation(func.value, node)
        if isinstance(func, ast.Attribute) and func.attr == "otherwise":
            return self._parse_otherwise(func.value, node)

        # ── .alias("name") ────────────────────────────────────────────
        if isinstance(func, ast.Attribute) and func.attr == "alias":
            inner = self._parse_expr(func.value)
            alias_name = self._eval_string(node.args[0])
            return Alias(expr=inner, name=alias_name)

        # ── .cast("type") ─────────────────────────────────────────────
        if isinstance(func, ast.Attribute) and func.attr == "cast":
            inner = self._parse_expr(func.value)
            type_str = self._eval_string(node.args[0])
            return Cast(expr=inner, target_type=type_str)

        # ── .isNull() / .isNotNull() ──────────────────────────────────
        if isinstance(func, ast.Attribute) and func.attr == "isNull":
            return IsNull(expr=self._parse_expr(func.value))
        if isinstance(func, ast.Attribute) and func.attr == "isNotNull":
            return IsNotNull(expr=self._parse_expr(func.value))

        # ── .isin([...]) / .isin(*args) ───────────────────────────────
        if isinstance(func, ast.Attribute) and func.attr == "isin":
            col_expr = self._parse_expr(func.value)
            if node.args and isinstance(node.args[0], ast.List):
                vals = tuple(self._parse_expr(e) for e in node.args[0].elts)
            else:
                vals = tuple(self._parse_expr(a) for a in node.args)
            return InList(expr=col_expr, values=vals)

        # ── col.asc() / col.desc() ────────────────────────────────────
        # (OrderSpec で扱うが、単独 Expr として現れることもある)
        if isinstance(func, ast.Attribute) and func.attr in ("asc", "desc"):
            return self._parse_expr(func.value)  # 方向は OrderSpec で処理

        # ── .over(window_spec) ────────────────────────────────────────
        if isinstance(func, ast.Attribute) and func.attr == "over":
            window_func = self._parse_expr(func.value)
            spec_node = node.args[0]
            # 変数参照の場合は _window_specs から取得
            if isinstance(spec_node, ast.Name) and spec_node.id in self._window_specs:
                window_spec = self._window_specs[spec_node.id]
            else:
                window_spec = self._parse_window_spec(spec_node)
            return WindowExpr(
                func=window_func,
                partition_by=window_spec["partition_by"],
                order_by=window_spec["order_by"],
                frame=window_spec.get("frame"),
            )

        # ── F.xxx(*args) 汎用関数 ─────────────────────────────────────
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id in ("F", "functions", "pyspark.sql.functions"):
                fn_name = func.attr
                # 集計・変換関数の引数内の文字列定数はカラム参照として扱う
                args = tuple(self._parse_col_expr(a) for a in node.args)
                kwargs = tuple(
                    (kw.arg, self._parse_col_expr(kw.value)) for kw in node.keywords
                )
                return FunctionCall(name=fn_name, args=args, kwargs=kwargs)

        # ── 汎用メソッド呼び出し ──────────────────────────────────────
        if isinstance(func, ast.Attribute):
            # メソッド名を関数名として扱う
            obj_expr = self._parse_expr(func.value)
            fn_name = func.attr
            args = tuple(self._parse_expr(a) for a in node.args)
            # obj をスコープに含めて FunctionCall にまとめる
            return FunctionCall(name=fn_name, args=(obj_expr,) + args)

        raise ConversionError(f"未対応の呼び出し: {ast.dump(node)}")

    # ------------------------------------------------------------------
    # CASE WHEN チェーン解析
    # ------------------------------------------------------------------

    def _parse_when_chain(self, node: ast.Call) -> CaseWhen:
        """F.when(cond, val) → CaseWhen の開始"""
        cond = self._parse_expr(node.args[0])
        val = self._parse_expr(node.args[1])
        return CaseWhen(branches=((cond, val),))

    def _parse_when_continuation(
        self, prev_node: ast.expr, call: ast.Call
    ) -> CaseWhen:
        """.when(cond, val) の追加"""
        prev = self._parse_expr(prev_node)
        if not isinstance(prev, CaseWhen):
            raise ConversionError(".when() は CaseWhen の後に続く必要があります")
        cond = self._parse_expr(call.args[0])
        val = self._parse_expr(call.args[1])
        return CaseWhen(branches=prev.branches + ((cond, val),), default=prev.default)

    def _parse_otherwise(self, prev_node: ast.expr, call: ast.Call) -> CaseWhen:
        """.otherwise(default) の解析"""
        prev = self._parse_expr(prev_node)
        if not isinstance(prev, CaseWhen):
            raise ConversionError(".otherwise() は CaseWhen の後に続く必要があります")
        default = self._parse_expr(call.args[0])
        return CaseWhen(branches=prev.branches, default=default)

    # ------------------------------------------------------------------
    # Window spec 解析
    # ------------------------------------------------------------------

    def _parse_window_spec(self, node: ast.expr) -> dict[str, Any]:
        """
        Window.partitionBy(...).orderBy(...).rowsBetween(...)
        を辞書にして返す
        """
        result: dict[str, Any] = {
            "partition_by": (),
            "order_by": (),
            "frame": None,
        }
        self._collect_window_spec(node, result)
        return result

    def _collect_window_spec(self, node: ast.expr, acc: dict) -> None:
        if not isinstance(node, ast.Call):
            return
        func = node.func
        if not isinstance(func, ast.Attribute):
            return

        method = func.attr
        self._collect_window_spec(func.value, acc)  # 先にチェーンを処理

        if method == "partitionBy":
            acc["partition_by"] = tuple(self._parse_col_expr(a) for a in node.args)
        elif method == "orderBy":
            acc["order_by"] = tuple(self._parse_order_spec(a) for a in node.args)
        elif method == "rowsBetween":
            start = self._eval_int_or_boundary(node.args[0])
            end = self._eval_int_or_boundary(node.args[1])
            acc["frame"] = WindowFrame(frame_type="ROWS", start=start, end=end)
        elif method == "rangeBetween":
            start = self._eval_int_or_boundary(node.args[0])
            end = self._eval_int_or_boundary(node.args[1])
            acc["frame"] = WindowFrame(frame_type="RANGE", start=start, end=end)

    # ------------------------------------------------------------------
    # 比較・ブール演算
    # ------------------------------------------------------------------

    def _parse_compare(self, node: ast.Compare) -> Expr:
        if len(node.ops) == 1:
            op = _CMPOP_MAP.get(type(node.ops[0]).__name__, "==")
            return BinaryOp(
                op=op,
                left=self._parse_expr(node.left),
                right=self._parse_expr(node.comparators[0]),
            )
        # a < b < c → (a < b) AND (b < c)
        parts = []
        prev = node.left
        for op_node, comp in zip(node.ops, node.comparators):
            op = _CMPOP_MAP.get(type(op_node).__name__, "==")
            parts.append(BinaryOp(op=op, left=self._parse_expr(prev), right=self._parse_expr(comp)))
            prev = comp
        result = parts[0]
        for part in parts[1:]:
            result = BinaryOp(op="AND", left=result, right=part)
        return result

    def _parse_boolop(self, node: ast.BoolOp) -> Expr:
        op = "AND" if isinstance(node.op, ast.And) else "OR"
        exprs = [self._parse_expr(v) for v in node.values]
        result = exprs[0]
        for e in exprs[1:]:
            result = BinaryOp(op=op, left=result, right=e)
        return result

    # ------------------------------------------------------------------
    # OrderSpec 解析
    # ------------------------------------------------------------------

    def _parse_order_spec(self, node: ast.expr) -> OrderSpec:
        """
        col / col.asc() / col.desc() / F.asc("col") / F.desc("col")
        → OrderSpec
        """
        ascending = True
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method == "asc":
                ascending = True
                node = node.func.value
            elif method == "asc_nulls_first":
                ascending = True
                node = node.func.value
            elif method == "desc":
                ascending = False
                node = node.func.value
            elif method == "desc_nulls_last":
                ascending = False
                node = node.func.value
        elif self._is_f_func(node, "asc") and isinstance(node, ast.Call):
            ascending = True
            node = node.args[0]
        elif self._is_f_func(node, "desc") and isinstance(node, ast.Call):
            ascending = False
            node = node.args[0] if isinstance(node, ast.Call) else node

        return OrderSpec(expr=self._parse_col_expr(node), ascending=ascending)

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------

    def _is_spark_attr(self, node: ast.expr, attr: str) -> bool:
        """node=Name(spark) かつ attr が一致するか判定"""
        # Case 1: obj=Name(id='spark'), attr はメソッド名として別途チェック
        if isinstance(node, ast.Name) and node.id == self.spark_var:
            return True  # attr は呼び出し元の method 変数で確認済み
        # Case 2: node 自体が spark.attr の Attribute ノード
        return (
            isinstance(node, ast.Attribute)
            and node.attr == attr
            and isinstance(node.value, ast.Name)
            and node.value.id == self.spark_var
        )

    def _is_spark_read(self, node: ast.expr) -> bool:
        return (
            isinstance(node, ast.Attribute)
            and node.attr == "read"
            and isinstance(node.value, ast.Name)
            and node.value.id == self.spark_var
        )

    def _is_f_func(self, node: ast.expr, name: str) -> bool:
        return (
            isinstance(node, ast.Attribute)
            and node.attr == name
            and isinstance(node.value, ast.Name)
            and node.value.id in ("F", "functions")
        )

    def _parse_string_arg(self, call: ast.Call, idx: int, context: str) -> str:
        if idx >= len(call.args):
            raise ConversionError(f"{context}: 引数が不足しています")
        return self._eval_string(call.args[idx])

    def _eval_string(self, node: ast.expr) -> str:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        raise ConversionError(f"文字列定数が期待されましたが: {ast.dump(node)}")

    def _eval_int(self, node: ast.expr) -> int:
        if isinstance(node, ast.Constant) and isinstance(node.value, int):
            return node.value
        raise ConversionError(f"整数定数が期待されましたが: {ast.dump(node)}")

    def _eval_int_or_boundary(self, node: ast.expr) -> int | str:
        """Window.unboundedPreceding などの境界値を処理"""
        if isinstance(node, ast.Attribute):
            if node.attr in ("unboundedPreceding", "unbounded_preceding"):
                return "UNBOUNDED PRECEDING"
            if node.attr in ("unboundedFollowing", "unbounded_following"):
                return "UNBOUNDED FOLLOWING"
            if node.attr in ("currentRow", "current_row"):
                return "CURRENT ROW"
        return self._eval_int(node)

    def _eval_literal(self, node: ast.expr) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.List):
            return [self._eval_literal(e) for e in node.elts]
        return None


# ---------------------------------------------------------------------------
# 演算子マッピング
# ---------------------------------------------------------------------------

_BINOP_MAP = {
    "Add": "+", "Sub": "-", "Mult": "*", "Div": "/",
    "FloorDiv": "//", "Mod": "%", "Pow": "**",
    "BitAnd": "&", "BitOr": "|", "BitXor": "^",
}
_CMPOP_MAP = {
    "Eq": "=", "NotEq": "!=", "Lt": "<", "LtE": "<=",
    "Gt": ">", "GtE": ">=",
}
_UNARYOP_MAP = {
    "Not": "NOT", "USub": "-", "Invert": "~",
}

# ---------------------------------------------------------------------------
# 内部ヘルパープラン (groupBy チェーンを一時保持)
# ---------------------------------------------------------------------------



