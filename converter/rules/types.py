"""
PySpark DataType → BigQuery 型名マッピング
"""
from __future__ import annotations

# PySpark 型名 (小文字) → BigQuery 型名
SPARK_TO_BQ: dict[str, str] = {
    # 整数系
    "byteType":    "INT64",
    "byte":        "INT64",
    "shortType":   "INT64",
    "short":       "INT64",
    "integerType": "INT64",
    "integer":     "INT64",
    "int":         "INT64",
    "longType":    "INT64",
    "long":        "INT64",
    "bigint":      "INT64",

    # 浮動小数点
    "floatType":   "FLOAT64",
    "float":       "FLOAT64",
    "doubleType":  "FLOAT64",
    "double":      "FLOAT64",
    "decimal":     "NUMERIC",
    "numeric":     "NUMERIC",

    # 文字列
    "stringType":  "STRING",
    "string":      "STRING",
    "varchar":     "STRING",
    "char":        "STRING",

    # 真偽値
    "booleanType": "BOOL",
    "boolean":     "BOOL",
    "bool":        "BOOL",

    # 日付・時刻
    "dateType":    "DATE",
    "date":        "DATE",
    "timestampType": "TIMESTAMP",
    "timestamp":   "TIMESTAMP",
    "timestamp_ntz": "DATETIME",

    # バイナリ
    "binaryType":  "BYTES",
    "binary":      "BYTES",

    # 複合型 (基本のみ)
    "arrayType":   "ARRAY",
    "array":       "ARRAY",
    "structType":  "STRUCT",
    "struct":      "STRUCT",
    "mapType":     "JSON",  # BQ は map を JSON で代替
    "map":         "JSON",
}


def convert_type(spark_type: str) -> str:
    """
    PySpark 型名文字列を BigQuery 型名に変換する。
    未知の型名はそのまま大文字で返す (手動確認用)。

    Examples:
        convert_type("string") → "STRING"
        convert_type("IntegerType") → "INT64"
        convert_type("decimal(18,3)") → "NUMERIC(18, 3)"
    """
    # decimal(precision, scale) のような引数付き型を処理
    if spark_type.lower().startswith("decimal"):
        import re
        m = re.match(r"decimal\((\d+),\s*(\d+)\)", spark_type, re.IGNORECASE)
        if m:
            return f"NUMERIC({m.group(1)}, {m.group(2)})"
        return "NUMERIC"

    # array<elementType> を処理
    if spark_type.lower().startswith("array<"):
        inner = spark_type[6:-1]
        return f"ARRAY<{convert_type(inner)}>"

    # map<k, v> を処理 (BQ では JSON 推奨)
    if spark_type.lower().startswith("map<"):
        return "JSON"

    # struct<...> を処理
    if spark_type.lower().startswith("struct<"):
        return spark_type  # 構造体は複雑なため現状そのまま返す

    return SPARK_TO_BQ.get(spark_type, SPARK_TO_BQ.get(spark_type.lower(), spark_type.upper()))

# alias for backward compatibility
spark_type_to_bq = convert_type

