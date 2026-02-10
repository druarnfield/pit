"""data.py — Inter-task data passing via Parquet files.

Tasks write named outputs as Parquet files into the run's data directory.
Downstream tasks read them back. The Go orchestrator can bulk-load
Parquet files into databases via the load_data RPC.
"""

import os

import pyarrow as pa
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


def write_output(name: str, data) -> str:
    """Write data to a named Parquet file in the run's data directory.

    Args:
        name: Output name (without extension). The file will be written
              as ``{data_dir}/{name}.parquet``.
        data: An Arrow Table, pandas DataFrame, or polars DataFrame.

    Returns:
        The absolute path to the written Parquet file.

    Raises:
        TypeError: If data is not a supported type.
        RuntimeError: If PIT_DATA_DIR is not set.
    """
    path = os.path.join(_data_dir(), f"{name}.parquet")

    if isinstance(data, pa.Table):
        pq.write_table(data, path)
    elif _is_pandas_df(data):
        table = pa.Table.from_pandas(data)
        pq.write_table(table, path)
    elif _is_polars_df(data):
        data.write_parquet(path)
    else:
        raise TypeError(
            f"Unsupported data type {type(data).__name__} — "
            "pass an Arrow Table, pandas DataFrame, or polars DataFrame"
        )

    return path


def read_input(name: str) -> pa.Table:
    """Read a named Parquet file from the run's data directory.

    Args:
        name: Output name (without extension). Reads from
              ``{data_dir}/{name}.parquet``.

    Returns:
        An Arrow Table.

    Raises:
        FileNotFoundError: If the Parquet file does not exist.
        RuntimeError: If PIT_DATA_DIR is not set.
    """
    path = os.path.join(_data_dir(), f"{name}.parquet")
    return pq.read_table(path)


def load_data(
    file: str,
    table: str,
    connection: str,
    *,
    schema: str = "dbo",
    mode: str = "append",
) -> str:
    """Trigger a Go-side bulk load of a Parquet file into a database table.

    This sends an RPC to the Go orchestrator's ``load_data`` handler,
    which reads the Parquet file and bulk-loads it using the native
    database driver (no ODBC required).

    Args:
        file: Parquet file name relative to the data directory
              (e.g. "output.parquet").
        table: Target table name.
        connection: Secret key for the connection string
                    (resolved from secrets store).
        schema: Target schema (default "dbo").
        mode: Load mode — "append", "truncate_and_load", or
              "create_or_replace" (drops and recreates the table
              from the Parquet schema).

    Returns:
        A message from the orchestrator (e.g. "1000 rows loaded").

    Raises:
        RuntimeError: If PIT_SOCKET is not set or the RPC fails.
    """
    from pit_sdk.secret import _request

    return _request(
        "load_data",
        {
            "file": file,
            "table": table,
            "connection": connection,
            "schema": schema,
            "mode": mode,
        },
    )


def _is_pandas_df(obj) -> bool:
    """Check if obj is a pandas DataFrame without importing pandas."""
    return type(obj).__module__.startswith("pandas") and type(obj).__name__ == "DataFrame"


def _is_polars_df(obj) -> bool:
    """Check if obj is a polars DataFrame without importing polars."""
    return type(obj).__module__.startswith("polars") and type(obj).__name__ == "DataFrame"
