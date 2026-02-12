"""FTP operations via the Pit orchestrator.

All functions communicate with the Go FTP client through the SDK socket.
Credentials are resolved from structured secrets — Python never sees passwords.
"""

import json

from pit_sdk.secret import _request


def ftp_list(secret: str, directory: str, pattern: str = "*") -> list[str]:
    """List files on an FTP server matching a glob pattern.

    Args:
        secret: Name of the structured secret (host, user, password, port, tls).
        directory: Remote directory to list.
        pattern: Glob pattern to filter filenames (default ``"*"``).

    Returns:
        List of matching filenames (names only, not full paths).
    """
    result = _request("ftp_list", {
        "secret": secret,
        "directory": directory,
        "pattern": pattern,
    })
    return json.loads(result)


def ftp_download(
    secret: str,
    remote_path: str,
    *,
    pattern: str | None = None,
) -> list[str]:
    """Download files from an FTP server into the run's data directory.

    Can operate in two modes:

    **Single file** — download one file by its full remote path::

        ftp_download("ftp_creds", "/incoming/sales/report.csv")

    **Pattern match** — download all matching files from a directory::

        ftp_download("ftp_creds", "/incoming/sales", pattern="*.csv")

    Args:
        secret: Name of the structured secret (host, user, password, port, tls).
        remote_path: Full path to a single file, or the directory when
            using ``pattern``.
        pattern: Glob pattern for batch download. When set, ``remote_path``
            is treated as the directory to list.

    Returns:
        List of downloaded filenames (relative to PIT_DATA_DIR).
    """
    params: dict[str, str] = {"secret": secret}
    if pattern is not None:
        params["directory"] = remote_path
        params["pattern"] = pattern
    else:
        params["remote_path"] = remote_path

    result = _request("ftp_download", params)
    return json.loads(result)


def ftp_upload(secret: str, local_name: str, remote_path: str) -> None:
    """Upload a file from the data directory to an FTP server.

    Args:
        secret: Name of the structured secret (host, user, password, port, tls).
        local_name: Filename in PIT_DATA_DIR to upload.
        remote_path: Full destination path on the FTP server.
    """
    _request("ftp_upload", {
        "secret": secret,
        "local_name": local_name,
        "remote_path": remote_path,
    })


def ftp_move(secret: str, src: str, dst: str) -> None:
    """Move or rename a file on an FTP server.

    Args:
        secret: Name of the structured secret (host, user, password, port, tls).
        src: Current remote path.
        dst: New remote path.
    """
    _request("ftp_move", {
        "secret": secret,
        "src": src,
        "dst": dst,
    })
