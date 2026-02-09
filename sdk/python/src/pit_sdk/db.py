"""db.py — Database read helpers using ConnectorX.

ConnectorX is a Rust-native database connector that reads query results
directly into Arrow, pandas, or polars without ODBC drivers.
"""

import connectorx as cx


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
