"""
PySpark 関数 → BigQuery SQL 関数マッピング (registry ベース)
各ハンドラの型: (args: list[str], kwargs: dict[str, str]) -> str
"""
from __future__ import annotations
from typing import Callable

FuncHandler = Callable[[list[str], dict[str, str]], str]
_REGISTRY: dict[str, FuncHandler] = {}


def register(*names: str):
    """デコレータ: 関数名をレジストリに登録する"""
    def decorator(fn: FuncHandler) -> FuncHandler:
        for name in names:
            _REGISTRY[name] = fn
        return fn
    return decorator


def get_handler(name: str) -> FuncHandler | None:
    return _REGISTRY.get(name)


# ─── NULL ──────────────────────────────────────────────────────────
@register("coalesce", "ifnull")
def _coalesce(a, k): return f"COALESCE({', '.join(a)})"

@register("nullif")
def _nullif(a, k): return f"NULLIF({a[0]}, {a[1]})"

@register("nvl")
def _nvl(a, k): return f"IFNULL({a[0]}, {a[1]})"


# ─── 文字列 ────────────────────────────────────────────────────────
@register("upper")
def _upper(a, k): return f"UPPER({a[0]})"

@register("lower")
def _lower(a, k): return f"LOWER({a[0]})"

@register("trim")
def _trim(a, k): return f"TRIM({a[0]})"

@register("ltrim")
def _ltrim(a, k): return f"LTRIM({a[0]})"

@register("rtrim")
def _rtrim(a, k): return f"RTRIM({a[0]})"

@register("length", "len")
def _length(a, k): return f"LENGTH({a[0]})"

@register("substr", "substring")
def _substr(a, k): return f"SUBSTR({', '.join(a)})"

@register("concat")
def _concat(a, k): return f"CONCAT({', '.join(a)})"

@register("concat_ws")
def _concat_ws(a, k):
    sep, cols = a[0], a[1:]
    return f"ARRAY_TO_STRING([{', '.join(cols)}], {sep})"

@register("split")
def _split(a, k): return f"SPLIT({a[0]}, {a[1]})"

@register("regexp_replace")
def _regexp_replace(a, k): return f"REGEXP_REPLACE({a[0]}, {a[1]}, {a[2]})"

@register("regexp_extract")
def _regexp_extract(a, k): return f"REGEXP_EXTRACT({a[0]}, {a[1]})"

@register("instr")
def _instr(a, k): return f"STRPOS({a[0]}, {a[1]})"

@register("lpad")
def _lpad(a, k): return f"LPAD({a[0]}, {a[1]}, {a[2]})"

@register("rpad")
def _rpad(a, k): return f"RPAD({a[0]}, {a[1]}, {a[2]})"

@register("repeat")
def _repeat(a, k): return f"REPEAT({a[0]}, {a[1]})"

@register("reverse")
def _reverse(a, k): return f"REVERSE({a[0]})"

@register("initcap")
def _initcap(a, k): return f"INITCAP({a[0]})"

@register("replace")
def _replace(a, k): return f"REPLACE({a[0]}, {a[1]}, {a[2]})"

@register("format_string")
def _format_string(a, k): return f"FORMAT({', '.join(a)})"


# ─── 数値 ──────────────────────────────────────────────────────────
@register("abs")
def _abs(a, k): return f"ABS({a[0]})"

@register("ceil", "ceiling")
def _ceil(a, k): return f"CEIL({a[0]})"

@register("floor")
def _floor(a, k): return f"FLOOR({a[0]})"

@register("round")
def _round(a, k): return f"ROUND({a[0]}, {a[1]})" if len(a) >= 2 else f"ROUND({a[0]})"

@register("sqrt")
def _sqrt(a, k): return f"SQRT({a[0]})"

@register("pow", "power")
def _pow(a, k): return f"POWER({a[0]}, {a[1]})"

@register("log")
def _log(a, k): return f"LOG({a[1]}, {a[0]})" if len(a) == 2 else f"LN({a[0]})"

@register("log2")
def _log2(a, k): return f"LOG({a[0]}, 2)"

@register("log10")
def _log10(a, k): return f"LOG10({a[0]})"

@register("exp")
def _exp(a, k): return f"EXP({a[0]})"

@register("sign", "signum")
def _sign(a, k): return f"SIGN({a[0]})"

@register("rand", "random")
def _rand(a, k): return "RAND()"

@register("greatest")
def _greatest(a, k): return f"GREATEST({', '.join(a)})"

@register("least")
def _least(a, k): return f"LEAST({', '.join(a)})"

@register("pmod")
def _pmod(a, k): return f"MOD({a[0]}, {a[1]})"


# ─── 日付・時刻 ────────────────────────────────────────────────────
@register("current_date")
def _current_date(a, k): return "CURRENT_DATE()"

@register("current_timestamp", "now")
def _current_timestamp(a, k): return "CURRENT_TIMESTAMP()"

@register("to_date")
def _to_date(a, k): return f"PARSE_DATE({a[1]}, {a[0]})" if len(a) >= 2 else f"DATE({a[0]})"

@register("to_timestamp")
def _to_timestamp(a, k): return f"PARSE_TIMESTAMP({a[1]}, {a[0]})" if len(a) >= 2 else f"TIMESTAMP({a[0]})"

@register("date_format")
def _date_format(a, k): return f"FORMAT_DATE({a[1]}, {a[0]})"

@register("date_add")
def _date_add(a, k): return f"DATE_ADD({a[0]}, INTERVAL {a[1]} DAY)"

@register("date_sub")
def _date_sub(a, k): return f"DATE_SUB({a[0]}, INTERVAL {a[1]} DAY)"

@register("datediff")
def _datediff(a, k): return f"DATE_DIFF({a[0]}, {a[1]}, DAY)"

@register("months_between")
def _months_between(a, k): return f"DATE_DIFF({a[0]}, {a[1]}, MONTH)"

@register("add_months")
def _add_months(a, k): return f"DATE_ADD({a[0]}, INTERVAL {a[1]} MONTH)"

_TRUNC_UNIT_MAP = {
    "year": "YEAR", "yyyy": "YEAR", "yy": "YEAR",
    "month": "MONTH", "mm": "MONTH", "mon": "MONTH",
    "week": "WEEK", "day": "DAY", "dd": "DAY",
    "hour": "HOUR", "minute": "MINUTE", "second": "SECOND",
}

@register("trunc")
def _trunc(a, k):
    raw = a[1].strip("\'" + '"')
    unit = _TRUNC_UNIT_MAP.get(raw.lower(), raw.upper())
    return f"DATE_TRUNC({a[0]}, {unit})"

@register("date_trunc")
def _date_trunc(a, k):
    raw = a[0].strip("\'" + '"')
    unit = _TRUNC_UNIT_MAP.get(raw.lower(), raw.upper())
    return f"TIMESTAMP_TRUNC({a[1]}, {unit})"

@register("year")
def _year(a, k): return f"EXTRACT(YEAR FROM {a[0]})"

@register("month")
def _month(a, k): return f"EXTRACT(MONTH FROM {a[0]})"

@register("dayofmonth", "day")
def _dayofmonth(a, k): return f"EXTRACT(DAY FROM {a[0]})"

@register("dayofweek")
def _dayofweek(a, k): return f"EXTRACT(DAYOFWEEK FROM {a[0]})"

@register("dayofyear")
def _dayofyear(a, k): return f"EXTRACT(DAYOFYEAR FROM {a[0]})"

@register("weekofyear")
def _weekofyear(a, k): return f"EXTRACT(WEEK FROM {a[0]})"

@register("hour")
def _hour(a, k): return f"EXTRACT(HOUR FROM {a[0]})"

@register("minute")
def _minute(a, k): return f"EXTRACT(MINUTE FROM {a[0]})"

@register("second")
def _second(a, k): return f"EXTRACT(SECOND FROM {a[0]})"

@register("quarter")
def _quarter(a, k): return f"EXTRACT(QUARTER FROM {a[0]})"

@register("unix_timestamp")
def _unix_timestamp(a, k):
    return f"UNIX_SECONDS(TIMESTAMP({a[0]}))" if a else "UNIX_SECONDS(CURRENT_TIMESTAMP())"

@register("from_unixtime")
def _from_unixtime(a, k):
    return f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_SECONDS(CAST({a[0]} AS INT64)))"

@register("last_day")
def _last_day(a, k):
    return f"DATE_SUB(DATE_TRUNC(DATE_ADD({a[0]}, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)"


# ─── 集計 ──────────────────────────────────────────────────────────
@register("count")
def _count(a, k):
    return "COUNT(*)" if not a or a[0] in ("'*'", '"*"', "*") else f"COUNT({a[0]})"

@register("count_distinct", "countDistinct")
def _count_distinct(a, k): return f"COUNT(DISTINCT {', '.join(a)})"

@register("sum")
def _sum(a, k): return f"SUM({a[0]})"

@register("sum_distinct", "sumDistinct")
def _sum_distinct(a, k): return f"SUM(DISTINCT {a[0]})"

@register("avg", "mean")
def _avg(a, k): return f"AVG({a[0]})"

@register("min")
def _min(a, k): return f"MIN({a[0]})"

@register("max")
def _max(a, k): return f"MAX({a[0]})"

@register("first")
def _first(a, k): return f"ANY_VALUE({a[0]})"

@register("collect_list")
def _collect_list(a, k): return f"ARRAY_AGG({a[0]} IGNORE NULLS)"

@register("collect_set")
def _collect_set(a, k): return f"ARRAY_AGG(DISTINCT {a[0]} IGNORE NULLS)"

@register("variance", "var_samp")
def _variance(a, k): return f"VAR_SAMP({a[0]})"

@register("var_pop")
def _var_pop(a, k): return f"VAR_POP({a[0]})"

@register("stddev", "stddev_samp")
def _stddev(a, k): return f"STDDEV_SAMP({a[0]})"

@register("stddev_pop")
def _stddev_pop(a, k): return f"STDDEV_POP({a[0]})"

@register("approx_count_distinct", "approxCountDistinct")
def _approx_count_distinct(a, k): return f"APPROX_COUNT_DISTINCT({a[0]})"

@register("corr")
def _corr(a, k): return f"CORR({a[0]}, {a[1]})"


# ─── Window ────────────────────────────────────────────────────────
@register("row_number")
def _row_number(a, k): return "ROW_NUMBER()"

@register("rank")
def _rank(a, k): return "RANK()"

@register("dense_rank")
def _dense_rank(a, k): return "DENSE_RANK()"

@register("percent_rank")
def _percent_rank(a, k): return "PERCENT_RANK()"

@register("cume_dist")
def _cume_dist(a, k): return "CUME_DIST()"

@register("ntile")
def _ntile(a, k): return f"NTILE({a[0]})"

@register("lag")
def _lag(a, k): return f"LAG({', '.join(a)})"

@register("lead")
def _lead(a, k): return f"LEAD({', '.join(a)})"

@register("first_value")
def _first_value(a, k): return f"FIRST_VALUE({a[0]})"

@register("last_value")
def _last_value(a, k): return f"LAST_VALUE({a[0]})"

@register("nth_value")
def _nth_value(a, k): return f"NTH_VALUE({a[0]}, {a[1]})"


# ─── 配列・構造体 ──────────────────────────────────────────────────
@register("array")
def _array(a, k): return f"[{', '.join(a)}]"

@register("array_contains")
def _array_contains(a, k): return f"{a[1]} IN UNNEST({a[0]})"

@register("array_join")
def _array_join(a, k):
    return f"ARRAY_TO_STRING({a[0]}, {a[1]}, {a[2]})" if len(a) > 2 else f"ARRAY_TO_STRING({a[0]}, {a[1]})"

@register("array_sort")
def _array_sort(a, k): return f"(SELECT ARRAY_AGG(x ORDER BY x) FROM UNNEST({a[0]}) AS x)"

@register("array_min")
def _array_min(a, k): return f"(SELECT MIN(x) FROM UNNEST({a[0]}) AS x)"

@register("array_max")
def _array_max(a, k): return f"(SELECT MAX(x) FROM UNNEST({a[0]}) AS x)"

@register("size", "cardinality")
def _size(a, k): return f"ARRAY_LENGTH({a[0]})"

@register("struct")
def _struct(a, k): return f"STRUCT({', '.join(a)})"

@register("flatten")
def _flatten(a, k):
    return f"(SELECT ARRAY_AGG(x) FROM UNNEST({a[0]}) AS arr, UNNEST(arr) AS x)"


# ─── JSON ──────────────────────────────────────────────────────────
@register("get_json_object")
def _get_json_object(a, k): return f"JSON_EXTRACT({a[0]}, {a[1]})"

@register("to_json")
def _to_json(a, k): return f"TO_JSON_STRING({a[0]})"

@register("from_json")
def _from_json(a, k): return f"PARSE_JSON({a[0]})"


# ─── Hash ──────────────────────────────────────────────────────────
@register("md5")
def _md5(a, k): return f"TO_HEX(MD5({a[0]}))"

@register("sha1")
def _sha1(a, k): return f"TO_HEX(SHA1({a[0]}))"

@register("hash", "xxhash64")
def _hash(a, k): return f"FARM_FINGERPRINT({a[0]})"


# ─── その他 ────────────────────────────────────────────────────────
@register("lit")
def _lit(a, k): return a[0] if a else "NULL"

@register("col", "column")
def _col(a, k): return a[0]

@register("broadcast")
def _broadcast(a, k): return a[0]  # BQ では不要

@register("monotonically_increasing_id")
def _mono_id(a, k):
    return "/* WARNING: monotonically_increasing_id not supported in BQ */ ROW_NUMBER() OVER ()"

@register("explode")
def _explode(a, k):
    return f"/* WARNING: explode requires CROSS JOIN UNNEST({a[0]}) in FROM clause */"

@register("posexplode")
def _posexplode(a, k):
    return f"/* WARNING: posexplode requires WITH OFFSET */ UNNEST({a[0]})"
