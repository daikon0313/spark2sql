"""
Emitter: IR ノードツリー → BigQuery SQL 文字列

責務:
  - Plan ツリーを再帰的に辿り、WITH 句や SELECT 文を組み立てる
  - Expr ノードを SQL の断片文字列に変換する
  - 関数マッピングは rules/functions.py のレジストリに委譲する
  - 型変換は rules/types.py に委譲する
"""
from __future__ import annotations

import re
from typing import Any

from .ir import (
    Alias, BinaryOp, Cast, CaseWhen, ColRef, ConversionError,
    DistinctPlan, DropPlan, Expr, FilterPlan, FunctionCall,
    GroupByPlan, InList, IsNotNull, IsNull, JoinPlan, JoinType,
    LimitPlan, Literal, OrderByPlan, OrderSpec, Plan,
    RenamePlan, SelectPlan, SourceTable, StarExpr, SubqueryPlan,
    UnaryOp, UnionPlan, WindowExpr, WindowFrame, WithColumnPlan,
)
from .rules.functions import get_handler
from .rules.types import convert_type


# ---------------------------------------------------------------------------
# SQL 生成コンテキスト
# ---------------------------------------------------------------------------

class _EmitContext:
    """SQL 生成中の状態を保持するコンテキスト"""

    def __init__(self, pretty: bool = True) -> None:
        self.pretty = pretty
        self.cte_counter = 0
        self.ctes: list[tuple[str, str]] = []   # [(name, sql), ...]
        self.warnings: list[str] = []

    def new_cte_name(self) -> str:
        self.cte_counter += 1
        return f"_cte_{self.cte_counter}"

    def add_warning(self, msg: str) -> None:
        if msg not in self.warnings:
            self.warnings.append(msg)


# ---------------------------------------------------------------------------
# メイン Emitter クラス
# ---------------------------------------------------------------------------

class SQLEmitter:
    """
    IR プランツリーを BigQuery SQL 文字列に変換する。

    Usage:
        emitter = SQLEmitter()
        result = emitter.emit(plan)
        print(result.sql)
        print(result.warnings)
    """

    def __init__(self, pretty: bool = True) -> None:
        self.pretty = pretty
        self._warnings: list[str] = []
        self._alias_counter = 0

    def emit(self, plan: Plan) -> tuple[str, list[str]]:
        """
        プランツリーを SQL 文字列に変換する。

        Returns:
            (sql_str, warnings)
        """
        ctx = _EmitContext()
        body = self._emit_plan(plan, ctx)

        if ctx.ctes:
            cte_clauses = ",\n".join(
                f"{name} AS (\n{self._indent(sql)}\n)" for name, sql in ctx.ctes
            )
            sql = f"WITH\n{cte_clauses}\n{body}"
        else:
            sql = body

        return sql, ctx.warnings

    # ------------------------------------------------------------------
    # Plan ノードの emit
    # ------------------------------------------------------------------

    def _emit_plan(self, plan: Plan, ctx: _EmitContext) -> str:
        if isinstance(plan, SourceTable):
            return self._emit_source(plan, ctx)
        if isinstance(plan, SelectPlan):
            return self._emit_select(plan, ctx)
        if isinstance(plan, FilterPlan):
            return self._emit_filter(plan, ctx)
        if isinstance(plan, GroupByPlan):
            return self._emit_groupby(plan, ctx)
        if isinstance(plan, JoinPlan):
            return self._emit_join(plan, ctx)
        if isinstance(plan, OrderByPlan):
            return self._emit_orderby(plan, ctx)
        if isinstance(plan, LimitPlan):
            return self._emit_limit(plan, ctx)
        if isinstance(plan, DistinctPlan):
            return self._emit_distinct(plan, ctx)
        if isinstance(plan, UnionPlan):
            return self._emit_union(plan, ctx)
        if isinstance(plan, WithColumnPlan):
            return self._emit_with_column(plan, ctx)
        if isinstance(plan, DropPlan):
            return self._emit_drop(plan, ctx)
        if isinstance(plan, RenamePlan):
            return self._emit_rename(plan, ctx)
        if isinstance(plan, SubqueryPlan):
            return self._emit_subquery(plan, ctx)
        raise ConversionError(f"未対応のプランノード: {type(plan).__name__}")

    def _emit_source(self, plan: SourceTable, ctx: _EmitContext) -> str:
        if plan.source_type == "table":
            name = plan.name
            alias = f" AS {plan.alias}" if plan.alias else ""
            return f"SELECT * FROM `{name}`{alias}"
        elif plan.source_type in ("parquet", "csv", "json", "orc"):
            ctx.add_warning(
                f"spark.read.{plan.source_type}('{plan.name}') は BQ の外部テーブル or "
                f"ロード済みテーブルに置き換えてください"
            )
            safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", plan.name.split("/")[-1].split(".")[0])
            return f"SELECT * FROM `{safe_name}`  -- TODO: replace with actual BQ table"
        elif plan.source_type == "reference":
            # 未解決の変数参照 → そのまま名前でテーブル参照
            return f"SELECT * FROM `{plan.name}`"
        return f"SELECT * FROM `{plan.name}`"

    def _emit_select(self, plan: SelectPlan, ctx: _EmitContext) -> str:
        # ソースが複雑な場合は CTE にする
        source_sql = self._as_subquery_or_cte(plan.source, ctx)
        cols = ", ".join(self._emit_expr(c, ctx) for c in plan.columns)
        return f"SELECT\n  {cols}\nFROM {source_sql}"

    def _emit_filter(self, plan: FilterPlan, ctx: _EmitContext) -> str:
        # シンプルな SELECT * + WHERE に変換
        # ソースがすでに SELECT なら CTE or サブクエリにする
        if isinstance(plan.source, (FilterPlan, SelectPlan, GroupByPlan,
                                     JoinPlan, OrderByPlan, LimitPlan,
                                     DistinctPlan, UnionPlan, WithColumnPlan,
                                     DropPlan, RenamePlan)):
            source_sql = self._to_cte(plan.source, ctx)
            cond = self._emit_expr(plan.condition, ctx)
            return f"SELECT *\nFROM {source_sql}\nWHERE {cond}"
        else:
            # SourceTable に対する直接 WHERE
            table_ref = self._source_table_ref(plan.source)
            cond = self._emit_expr(plan.condition, ctx)
            return f"SELECT *\nFROM {table_ref}\nWHERE {cond}"

    def _emit_groupby(self, plan: GroupByPlan, ctx: _EmitContext) -> str:
        source_sql = self._as_subquery_or_cte(plan.source, ctx)

        agg_cols = [self._emit_expr(a, ctx) for a in plan.aggregations]

        if plan.keys:
            key_cols = [self._emit_expr(k, ctx) for k in plan.keys]
            key_str = ", ".join(key_cols)
            select_cols = key_cols + agg_cols
            return (
                f"SELECT\n  {', '.join(select_cols)}\n"
                f"FROM {source_sql}\n"
                f"GROUP BY {key_str}"
            )
        else:
            # 全行集計 (GROUP BY なし)
            return (
                f"SELECT\n  {', '.join(agg_cols)}\n"
                f"FROM {source_sql}"
            )

    def _emit_join(self, plan: JoinPlan, ctx: _EmitContext) -> str:
        left_sql = self._as_subquery_or_cte(plan.left, ctx)
        right_sql = self._as_subquery_or_cte(plan.right, ctx)

        join_kw = {
            JoinType.INNER:     "INNER JOIN",
            JoinType.LEFT:      "LEFT JOIN",
            JoinType.RIGHT:     "RIGHT JOIN",
            JoinType.FULL:      "FULL OUTER JOIN",
            JoinType.LEFT_SEMI: "LEFT JOIN",   # BQ は SEMI JOIN 構文なし → EXISTS で代替警告
            JoinType.LEFT_ANTI: "LEFT JOIN",   # 同上
            JoinType.CROSS:     "CROSS JOIN",
        }[plan.join_type]

        if plan.join_type == JoinType.LEFT_SEMI:
            ctx.add_warning("LEFT SEMI JOIN は BQ にありません。EXISTS サブクエリへの書き換えを検討してください")
        if plan.join_type == JoinType.LEFT_ANTI:
            ctx.add_warning("LEFT ANTI JOIN は BQ にありません。NOT EXISTS サブクエリへの書き換えを検討してください")

        if plan.join_type == JoinType.CROSS:
            return f"SELECT *\nFROM {left_sql}\nCROSS JOIN {right_sql}"

        if plan.using_columns:
            using = ", ".join(plan.using_columns)
            return f"SELECT *\nFROM {left_sql}\n{join_kw} {right_sql} USING ({using})"

        if plan.condition:
            cond = self._emit_expr(plan.condition, ctx)
            return f"SELECT *\nFROM {left_sql}\n{join_kw} {right_sql} ON {cond}"

        raise ConversionError("JoinPlan に condition も using_columns もありません")

    def _emit_orderby(self, plan: OrderByPlan, ctx: _EmitContext) -> str:
        source_sql = self._as_subquery_or_cte(plan.source, ctx)
        order_parts = [self._emit_order_spec(s, ctx) for s in plan.order_specs]
        return f"SELECT *\nFROM {source_sql}\nORDER BY {', '.join(order_parts)}"

    def _emit_limit(self, plan: LimitPlan, ctx: _EmitContext) -> str:
        source_sql = self._as_subquery_or_cte(plan.source, ctx)
        return f"SELECT *\nFROM {source_sql}\nLIMIT {plan.n}"

    def _emit_distinct(self, plan: DistinctPlan, ctx: _EmitContext) -> str:
        if plan.subset:
            # dropDuplicates(subset) → ROW_NUMBER() でサロゲート
            ctx.add_warning(
                f"dropDuplicates({list(plan.subset)}) は ROW_NUMBER() OVER (PARTITION BY ...) で実装されます"
            )
            source_cte = self._to_cte(plan.source, ctx)
            partition_cols = ", ".join(plan.subset)
            dedup_cte = ctx.new_cte_name()
            ctx.ctes.append((dedup_cte, (
                f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_cols}) AS _rn\n"
                f"FROM {source_cte}"
            )))
            return f"SELECT * EXCEPT(_rn)\nFROM {dedup_cte}\nWHERE _rn = 1"
        else:
            source_sql = self._as_subquery_or_cte(plan.source, ctx)
            return f"SELECT DISTINCT *\nFROM {source_sql}"

    def _emit_union(self, plan: UnionPlan, ctx: _EmitContext) -> str:
        union_kw = "UNION DISTINCT" if plan.distinct else "UNION ALL"
        parts = []
        for src in plan.sources:
            parts.append(self._as_subquery_or_cte(src, ctx, wrap_select=True))
        return f"\n{union_kw}\n".join(
            f"SELECT * FROM {p}" if not p.strip().upper().startswith("SELECT") else p
            for p in parts
        )

    def _emit_with_column(self, plan: WithColumnPlan, ctx: _EmitContext) -> str:
        # withColumn → SELECT *, new_expr AS col_name
        source_sql = self._as_subquery_or_cte(plan.source, ctx)
        expr_sql = self._emit_expr(plan.expr, ctx)
        return (
            f"SELECT\n  *,\n  {expr_sql} AS `{plan.column_name}`\n"
            f"FROM {source_sql}"
        )

    def _emit_drop(self, plan: DropPlan, ctx: _EmitContext) -> str:
        # drop → SELECT * EXCEPT(col1, col2)
        source_sql = self._as_subquery_or_cte(plan.source, ctx)
        cols = ", ".join(f"`{c}`" for c in plan.columns)
        return f"SELECT * EXCEPT({cols})\nFROM {source_sql}"

    def _emit_rename(self, plan: RenamePlan, ctx: _EmitContext) -> str:
        # withColumnRenamed → SELECT * REPLACE(old AS new) ... BQ では EXCEPT + alias
        ctx.add_warning(
            f"withColumnRenamed('{plan.old_name}', '{plan.new_name}') は "
            f"SELECT * EXCEPT と alias で代替します"
        )
        source_cte = self._to_cte(plan.source, ctx)
        return (
            f"SELECT\n"
            f"  * EXCEPT(`{plan.old_name}`),\n"
            f"  `{plan.old_name}` AS `{plan.new_name}`\n"
            f"FROM {source_cte}"
        )

    def _emit_subquery(self, plan: SubqueryPlan, ctx: _EmitContext) -> str:
        inner_sql = self._emit_plan(plan.source, ctx)
        return f"(\n{self._indent(inner_sql)}\n) AS {plan.alias}"

    # ------------------------------------------------------------------
    # Expr ノードの emit
    # ------------------------------------------------------------------

    def _emit_expr(self, expr: Expr, ctx: _EmitContext) -> str:  # noqa: C901
        if isinstance(expr, ColRef):
            return self._emit_col_ref(expr)

        if isinstance(expr, Literal):
            return self._emit_literal(expr)

        if isinstance(expr, Alias):
            inner = self._emit_expr(expr.expr, ctx)
            return f"{inner} AS `{expr.name}`"

        if isinstance(expr, BinaryOp):
            return self._emit_binary_op(expr, ctx)

        if isinstance(expr, UnaryOp):
            inner = self._emit_expr(expr.expr, ctx)
            if expr.op in ("NOT", "~"):
                return f"NOT ({inner})"
            return f"{expr.op}{inner}"

        if isinstance(expr, FunctionCall):
            return self._emit_function_call(expr, ctx)

        if isinstance(expr, CaseWhen):
            return self._emit_case_when(expr, ctx)

        if isinstance(expr, WindowExpr):
            return self._emit_window_expr(expr, ctx)

        if isinstance(expr, StarExpr):
            if expr.table_alias:
                return f"{expr.table_alias}.*"
            return "*"

        if isinstance(expr, Cast):
            inner = self._emit_expr(expr.expr, ctx)
            bq_type = convert_type(expr.target_type)
            return f"CAST({inner} AS {bq_type})"

        if isinstance(expr, IsNull):
            inner = self._emit_expr(expr.expr, ctx)
            return f"{inner} IS NULL"

        if isinstance(expr, IsNotNull):
            inner = self._emit_expr(expr.expr, ctx)
            return f"{inner} IS NOT NULL"

        if isinstance(expr, InList):
            col = self._emit_expr(expr.expr, ctx)
            vals = ", ".join(self._emit_expr(v, ctx) for v in expr.values)
            return f"{col} IN ({vals})"

        raise ConversionError(f"未対応の式ノード: {type(expr).__name__}")

    def _emit_col_ref(self, expr: ColRef) -> str:
        if expr.name == "*":
            if expr.table_alias:
                return f"`{expr.table_alias}`.*"
            return "*"
        if expr.table_alias:
            return f"`{expr.table_alias}`.`{expr.name}`"
        # ドット区切りのカラム名 (t1.col_a 形式)
        if "." in expr.name:
            parts = expr.name.split(".", 1)
            return f"`{parts[0]}`.`{parts[1]}`"
        return f"`{expr.name}`"

    def _emit_literal(self, expr: Literal) -> str:
        v = expr.value
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, str):
            escaped = v.replace("'", "\\'")
            return f"'{escaped}'"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, list):
            # リスト → ARRAY[]
            items = ", ".join(
                self._emit_literal(i) if isinstance(i, Literal) else str(i)
                for i in v
            )
            return f"[{items}]"
        return repr(v)

    def _emit_binary_op(self, expr: BinaryOp, ctx: _EmitContext) -> str:
        left = self._emit_expr(expr.left, ctx)
        right = self._emit_expr(expr.right, ctx)

        # 演算子を BQ SQL 構文に変換
        op_map = {
            "=":   "=",
            "==":  "=",
            "!=":  "!=",
            "<>":  "!=",
            ">":   ">",
            ">=":  ">=",
            "<":   "<",
            "<=":  "<=",
            "+":   "+",
            "-":   "-",
            "*":   "*",
            "/":   "/",
            "%":   "MOD",    # BQ は % の代わりに MOD
            "&":   "AND",
            "|":   "OR",
            "AND": "AND",
            "OR":  "OR",
        }
        bq_op = op_map.get(expr.op, expr.op)

        # AND / OR は括弧で囲む
        if bq_op in ("AND", "OR"):
            # 左右が BinaryOp で同じ論理演算子なら括弧不要だが安全のため付ける
            return f"({left} {bq_op} {right})"

        # MOD は関数形式
        if bq_op == "MOD":
            return f"MOD({left}, {right})"

        # 除算は SAFE_DIVIDE を使うオプションがあるが標準は /
        return f"{left} {bq_op} {right}"

    def _emit_function_call(self, expr: FunctionCall, ctx: _EmitContext) -> str:
        args = [self._emit_expr(a, ctx) for a in expr.args]
        kwargs = {k: self._emit_expr(v, ctx) if isinstance(v, Expr) else str(v)
                  for k, v in expr.kwargs}

        handler = get_handler(expr.name)
        if handler is not None:
            if callable(handler):
                return handler(args, kwargs)
            # str テンプレートの場合
            template = handler
            if "{}" in template or "{0}" in template:
                try:
                    return template.format(*args)
                except (IndexError, KeyError):
                    return template.replace("{}", args[0] if args else "")
            # 単純な関数名置換
            args_str = ", ".join(args)
            return f"{template}({args_str})"

        # ハンドラ未登録の場合は警告を出しつつそのまま変換
        ctx.add_warning(f"未登録の PySpark 関数: {expr.name}() → そのまま変換しました")
        args_str = ", ".join(args)
        return f"{expr.name.upper()}({args_str})"

    def _emit_case_when(self, expr: CaseWhen, ctx: _EmitContext) -> str:
        parts = ["CASE"]
        for cond, val in expr.branches:
            c = self._emit_expr(cond, ctx)
            v = self._emit_expr(val, ctx)
            parts.append(f"  WHEN {c} THEN {v}")
        if expr.default is not None:
            d = self._emit_expr(expr.default, ctx)
            parts.append(f"  ELSE {d}")
        parts.append("END")
        return "\n".join(parts)

    def _emit_window_expr(self, expr: WindowExpr, ctx: _EmitContext) -> str:
        func_sql = self._emit_expr(expr.func, ctx)

        over_parts = []

        if expr.partition_by:
            pb = ", ".join(self._emit_expr(e, ctx) for e in expr.partition_by)
            over_parts.append(f"PARTITION BY {pb}")

        if expr.order_by:
            ob = ", ".join(self._emit_order_spec(s, ctx) for s in expr.order_by)
            over_parts.append(f"ORDER BY {ob}")

        if expr.frame:
            frame_sql = self._emit_window_frame(expr.frame)
            over_parts.append(frame_sql)

        over_clause = " ".join(over_parts)
        return f"{func_sql} OVER ({over_clause})"

    def _emit_window_frame(self, frame: WindowFrame) -> str:
        def boundary(v: int | str) -> str:
            if isinstance(v, str):
                return v  # "UNBOUNDED PRECEDING" etc.
            if v == 0:
                return "CURRENT ROW"
            if v < 0:
                return f"{abs(v)} PRECEDING"
            return f"{v} FOLLOWING"

        return f"{frame.frame_type} BETWEEN {boundary(frame.start)} AND {boundary(frame.end)}"

    def _emit_order_spec(self, spec: OrderSpec, ctx: _EmitContext) -> str:
        col = self._emit_expr(spec.expr, ctx)
        direction = "ASC" if spec.ascending else "DESC"
        nulls = "NULLS LAST" if spec.nulls_last else "NULLS FIRST"
        return f"{col} {direction} {nulls}"

    # ------------------------------------------------------------------
    # サブクエリ / CTE ヘルパー
    # ------------------------------------------------------------------

    def _as_subquery_or_cte(
        self, plan: Plan, ctx: _EmitContext, wrap_select: bool = False
    ) -> str:
        """
        プランを FROM 句で使える形式に変換する。
        - SourceTable は直接テーブル名を返す
        - それ以外は CTE に昇格させる
        """
        if isinstance(plan, SourceTable):
            return self._source_table_ref(plan)
        return self._to_cte(plan, ctx)

    def _source_table_ref(self, plan: SourceTable) -> str:
        alias = f" AS {plan.alias}" if plan.alias else ""
        if plan.source_type == "reference":
            return f"`{plan.name}`{alias}"
        return f"`{plan.name}`{alias}"

    def _to_cte(self, plan: Plan, ctx: _EmitContext) -> str:
        """プランを CTE に変換して名前を返す"""
        sql = self._emit_plan(plan, ctx)
        name = ctx.new_cte_name()
        ctx.ctes.append((name, sql))
        return name

    def _indent(self, sql: str, spaces: int = 2) -> str:
        pad = " " * spaces
        return "\n".join(pad + line for line in sql.splitlines())
