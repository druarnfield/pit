"""db.py — Database read helpers using ConnectorX.

ConnectorX is a Rust-native database connector that reads query results
directly into Arrow, pandas, or polars without ODBC drivers.
"""

import os

import connectorx as cx
import pyarrow.parquet as pq


def _data_dir() -> str:
    """Return the run's data directory from PIT_DATA_DIR."""
    d = os.environ.get("PIT_DATA_DIR")
    if not d:
        raise RuntimeError(
            "PIT_DATA_DIR environment variable not set — "
            "are you running inside a Pit task?"
        )
    return d


def read_sql(
    conn: str,
    query: str,
    *,
    return_type: str = "arrow",
):
    """Execute a SQL query and return the results.

    Args:
        conn: Database connection string (e.g. "mssql://user:pass@host/db").
        query: SQL query to execute.
        return_type: Output format — "arrow" (default), "pandas", or "polars".

    Returns:
        Arrow Table, pandas DataFrame, or polars DataFrame depending on return_type.

    Raises:
        ValueError: If return_type is not recognised.
    """
    if return_type == "arrow":
        return cx.read_sql(conn, query, return_type="arrow")
    elif return_type == "pandas":
        return cx.read_sql(conn, query, return_type="pandas")
    elif return_type == "polars":
        return cx.read_sql(conn, query, return_type="polars")
    else:
        raise ValueError(
            f"Unsupported return_type {return_type!r} — "
            "use 'arrow', 'pandas', or 'polars'"
        )


def output_sql(
    conn: str,
    query: str,
    name: str,
) -> str:
    """Execute a SQL query and write results directly to Parquet on disk.

    Combines ``read_sql`` and ``write_output`` into a single step. The Arrow
    Table is written to disk and freed immediately — it never lives in the
    caller's scope, so Python can reclaim the memory straight away.

    Args:
        conn: Database connection string (e.g. "mssql://user:pass@host/db").
        query: SQL query to execute.
        name: Output name (without extension). The file will be written
              as ``{data_dir}/{name}.parquet``.

    Returns:
        The absolute path to the written Parquet file.

    Raises:
        RuntimeError: If PIT_DATA_DIR is not set.
    """
    table = cx.read_sql(conn, query, return_type="arrow")
    path = os.path.join(_data_dir(), f"{name}.parquet")
    pq.write_table(table, path)
    return path
