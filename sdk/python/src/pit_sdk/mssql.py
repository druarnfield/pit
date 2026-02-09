"""mssql.py — lightweight MSSQL helpers for data pipeline tasks."""

from contextlib import contextmanager

import pandas as pd
import pyodbc


def connection_string(
    host: str,
    database: str,
    *,
    driver: str = "ODBC Driver 17 for SQL Server",
    user: str | None = None,
    password: str | None = None,
) -> str:
    base = f"Driver={{{driver}}};Server={host};Database={database};"
    if user:
        return base + f"UID={user};PWD={password};"
    return base + "Trusted_Connection=yes;"


@contextmanager
def connect(host: str, database: str, **kwargs):
    """Yield a pyodbc connection, closed on exit."""
    con = pyodbc.connect(connection_string(host, database, **kwargs))
    try:
        yield con
    finally:
        con.close()


def _qualified(table: str, schema: str = "dbo") -> str:
    return f"[{schema}].[{table}]"


# --- Read ---


def read_table(con, table: str, *, schema: str = "dbo") -> pd.DataFrame:
    cursor = con.execute(f"SELECT * FROM {_qualified(table, schema)}")
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)


def read_query(con, sql: str, params: tuple | None = None) -> pd.DataFrame:
    cursor = con.execute(sql, params or ())
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)


def fetchall(con, sql: str, params: tuple | None = None) -> list[dict]:
    """Run a query, return list of dicts (no pandas dependency)."""
    cursor = con.execute(sql, params or ())
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetchone(con, sql: str, params: tuple | None = None) -> dict | None:
    cursor = con.execute(sql, params or ())
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row))


def scalar(con, sql: str, params: tuple | None = None):
    """Return a single value."""
    cursor = con.execute(sql, params or ())
    row = cursor.fetchone()
    return row[0] if row else None


# --- Write ---


def execute(con, sql: str, params: tuple | None = None) -> int:
    """Execute a statement, return rows affected."""
    cursor = con.execute(sql, params or ())
    rowcount = cursor.rowcount
    con.commit()
    return rowcount


def execute_many(con, sql: str, param_list: list[tuple]) -> int:
    """Execute a parameterized statement for each param tuple."""
    cursor = con.cursor()
    cursor.fast_executemany = True
    cursor.executemany(sql, param_list)
    rowcount = cursor.rowcount
    con.commit()
    return rowcount


def bulk_insert(con, table: str, df: pd.DataFrame, *, schema: str = "dbo") -> int:
    """Insert a DataFrame into a table using fast_executemany."""
    cols = ", ".join(f"[{c}]" for c in df.columns)
    placeholders = ", ".join("?" * len(df.columns))
    sql = f"INSERT INTO {_qualified(table, schema)} ({cols}) VALUES ({placeholders})"
    return execute_many(con, sql, df.values.tolist())


def truncate_and_load(con, table: str, df: pd.DataFrame, *, schema: str = "dbo") -> int:
    """Truncate table, then bulk insert. Common full-refresh pattern."""
    execute(con, f"TRUNCATE TABLE {_qualified(table, schema)}")
    return bulk_insert(con, table, df, schema=schema)


# --- Introspection ---


def table_exists(con, table: str, *, schema: str = "dbo") -> bool:
    return (
        scalar(
            con,
            "SELECT OBJECT_ID(?, 'U')",
            (f"{schema}.{table}",),
        )
        is not None
    )


def row_count(con, table: str, *, schema: str = "dbo") -> int:
    """Fast row count via sys.partitions (no table scan)."""
    return scalar(
        con,
        """
        SELECT SUM(p.rows)
        FROM sys.partitions p
        JOIN sys.tables t ON p.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = ? AND s.name = ? AND p.index_id IN (0, 1)
    """,
        (table, schema),
    )


def columns(con, table: str, *, schema: str = "dbo") -> list[dict]:
    """Return column metadata for a table."""
    return fetchall(
        con,
        """
        SELECT c.name, t.name AS type, c.max_length, c.is_nullable
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        JOIN sys.tables tbl ON c.object_id = tbl.object_id
        JOIN sys.schemas s ON tbl.schema_id = s.schema_id
        WHERE tbl.name = ? AND s.name = ?
        ORDER BY c.column_id
    """,
        (table, schema),
    )


# --- Pipeline utilities ---


def row_count_check(con, table: str, *, schema: str = "dbo", min_rows: int = 1) -> bool:
    """Simple data quality gate — does the table have enough rows?"""
    return row_count(con, table, schema=schema) >= min_rows


def freshness_check(
    con, table: str, column: str, *, schema: str = "dbo", max_age_hours: int = 24
) -> bool:
    """Check if the most recent value in a datetime column is within threshold."""
    age = scalar(
        con,
        f"SELECT DATEDIFF(HOUR, MAX([{column}]), GETDATE()) FROM {_qualified(table, schema)}",
    )
    return age is not None and age <= max_age_hours
