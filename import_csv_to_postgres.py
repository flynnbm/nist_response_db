from __future__ import annotations

import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    create_engine,
)
from sqlalchemy.engine import Engine
from sqlalchemy.sql.sqltypes import TypeEngine

from config import CSV_DIR, DATABASE_URL


# ----------------------------
# Name cleanup helpers
# ----------------------------

def normalize_name(name: str) -> str:
    """
    Convert a filename or column name into a PostgreSQL-friendly identifier.
    Example:
        'Part Number (%)' -> 'part_number'
    """
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "unnamed"
    if name[0].isdigit():
        name = f"col_{name}"
    return name


def unique_column_names(columns: list[str]) -> list[str]:
    """
    Ensure normalized column names remain unique.
    Example:
        ['part', 'part', 'part'] -> ['part', 'part_2', 'part_3']
    """
    seen: dict[str, int] = {}
    result: list[str] = []

    for col in columns:
        if col not in seen:
            seen[col] = 1
            result.append(col)
        else:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")

    return result


# ----------------------------
# Value parsing helpers
# ----------------------------

TRUE_VALUES = {"true", "t", "yes", "y", "1"}
FALSE_VALUES = {"false", "f", "no", "n", "0"}


def clean_string_value(value: Any) -> Any:
    """
    Normalize blank-ish values to None and trim strings.
    """
    if pd.isna(value):
        return None

    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None

    return value


def try_parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        if value == 1:
            return True
        if value == 0:
            return False

    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in TRUE_VALUES:
            return True
        if lowered in FALSE_VALUES:
            return False

    return None


def try_parse_int(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None

    if isinstance(value, str):
        s = value.strip()
        if re.fullmatch(r"[+-]?\d+", s):
            try:
                return int(s)
            except ValueError:
                return None

    return None


def try_parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None

    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if re.fullmatch(r"[+-]?\d+(\.\d+)?", s):
            try:
                return Decimal(s)
            except InvalidOperation:
                return None

    return None


def try_parse_datetime_series(series: pd.Series) -> pd.Series | None:
    """
    Try to parse a series as datetime.
    Returns a parsed series if successful enough, otherwise None.
    """
    parsed = pd.to_datetime(series, errors="coerce")

    non_null_original = series.notna().sum()
    non_null_parsed = parsed.notna().sum()

    if non_null_original == 0:
        return None

    success_ratio = non_null_parsed / non_null_original
    if success_ratio >= 0.90:
        return parsed

    return None


def is_date_only(parsed: pd.Series) -> bool:
    """
    Check whether parsed datetimes all appear to be date-only values.
    """
    non_null = parsed.dropna()
    if non_null.empty:
        return False

    return all(
        ts.hour == 0 and ts.minute == 0 and ts.second == 0 and ts.microsecond == 0
        for ts in non_null
    )


# ----------------------------
# Type detection
# ----------------------------

def detect_sqlalchemy_type(series: pd.Series) -> tuple[TypeEngine, pd.Series]:
    """
    Detect the best SQLAlchemy column type for a pandas Series.
    Returns:
        (sqlalchemy_type, converted_series)
    """
    cleaned = series.map(clean_string_value)

    non_null = cleaned.dropna()
    if non_null.empty:
        return Text(), cleaned

    # Preserve obvious leading-zero codes as text, e.g. "00123"
    if all(isinstance(v, str) for v in non_null):
        if any(re.fullmatch(r"0\d+", v.strip()) for v in non_null):
            return Text(), cleaned

    # Boolean
    bool_attempt = cleaned.map(try_parse_bool)
    if bool_attempt.dropna().shape[0] == non_null.shape[0]:
        return Boolean(), bool_attempt

    # Integer
    int_attempt = cleaned.map(try_parse_int)
    if int_attempt.dropna().shape[0] == non_null.shape[0]:
        return Integer(), int_attempt

    # Numeric / Decimal
    dec_attempt = cleaned.map(try_parse_decimal)
    if dec_attempt.dropna().shape[0] == non_null.shape[0]:
        max_scale = 0
        max_precision = 1

        for val in dec_attempt.dropna():
            sign, digits, exponent = val.as_tuple()
            digits_count = len(digits)

            if exponent < 0:
                scale = abs(exponent)
            else:
                scale = 0

            precision = max(digits_count, scale)
            max_scale = max(max_scale, scale)
            max_precision = max(max_precision, precision)

        # Keep some reasonable floor/ceiling
        precision = min(max(max_precision + 2, 10), 38)
        scale = min(max_scale, 12)

        return Numeric(precision=precision, scale=scale), dec_attempt

    # Date / DateTime
    if all(isinstance(v, str) for v in non_null):
        dt_attempt = try_parse_datetime_series(cleaned)
        if dt_attempt is not None:
            if is_date_only(dt_attempt):
                return Date(), dt_attempt.dt.date
            return DateTime(), dt_attempt

    # Fallback
    return Text(), cleaned


# ----------------------------
# DataFrame cleanup
# ----------------------------

def load_and_clean_csv(csv_path: Path) -> pd.DataFrame:
    """
    Load CSV with initial string preservation so we can control inference ourselves.
    """
    df = pd.read_csv(csv_path, dtype=str)

    # Normalize headers
    normalized = [normalize_name(str(col)) for col in df.columns]
    df.columns = unique_column_names(normalized)

    # Trim whitespace in string cells
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Replace blank strings with NA
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # Drop fully empty rows and columns
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    return df


def build_typed_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, TypeEngine]]:
    typed_df = pd.DataFrame()
    column_types: dict[str, TypeEngine] = {}

    for col in df.columns:
        detected_type, converted = detect_sqlalchemy_type(df[col])
        typed_df[col] = converted
        column_types[col] = detected_type

    return typed_df, column_types


# ----------------------------
# PostgreSQL upload
# ----------------------------

def create_table_for_dataframe(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    column_types: dict[str, TypeEngine],
) -> None:
    metadata = MetaData()

    columns = [Column("id", Integer, primary_key=True, autoincrement=True)]
    for col in df.columns:
        columns.append(Column(col, column_types[col], nullable=True))

    table = Table(table_name, metadata, *columns)

    # Drop and recreate for now to keep the workflow simple
    metadata.drop_all(engine, tables=[table], checkfirst=True)
    metadata.create_all(engine, tables=[table])


def insert_dataframe(engine: Engine, table_name: str, df: pd.DataFrame) -> None:
    """
    Insert rows via pandas to_sql in append mode after creating the table explicitly.
    """
    # Convert pandas NA/NaT to None for cleaner inserts
    rows_df = df.where(pd.notna(df), None)
    rows_df.to_sql(table_name, engine, if_exists="append", index=False)


def import_csv_file(engine: Engine, csv_path: Path) -> None:
    table_name = normalize_name(csv_path.stem)

    print(f"\nImporting: {csv_path.name}")
    print(f"Target table: {table_name}")

    df = load_and_clean_csv(csv_path)

    if df.empty:
        print("  Skipped: no data after cleaning.")
        return

    typed_df, column_types = build_typed_dataframe(df)

    print("  Detected columns/types:")
    for col, typ in column_types.items():
        print(f"    - {col}: {typ}")

    create_table_for_dataframe(engine, table_name, typed_df, column_types)
    insert_dataframe(engine, table_name, typed_df)

    print(f"  Imported {len(typed_df)} rows into '{table_name}'.")


def gather_csv_files(target: Path) -> list[Path]:
    if target.is_file() and target.suffix.lower() == ".csv":
        return [target]

    if target.is_dir():
        return sorted(target.glob("*.csv"))

    return []


def main() -> None:
    """
    Usage:
        python import_csv_to_postgres.py
        python import_csv_to_postgres.py path/to/file.csv
        python import_csv_to_postgres.py path/to/folder
    """
    if len(sys.argv) > 1:
        target = Path(sys.argv[1]).expanduser().resolve()
    else:
        target = CSV_DIR.resolve()

    csv_files = gather_csv_files(target)

    if not csv_files:
        print(f"No CSV files found at: {target}")
        sys.exit(1)

    engine = create_engine(DATABASE_URL, future=True)

    print(f"Using database: {DATABASE_URL}")
    print(f"Found {len(csv_files)} CSV file(s).")

    for csv_file in csv_files:
        try:
            import_csv_file(engine, csv_file)
        except Exception as exc:
            print(f"  ERROR importing {csv_file.name}: {exc}")

    print("\nDone.")


if __name__ == "__main__":
    main()