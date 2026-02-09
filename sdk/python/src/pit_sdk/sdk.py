"""Pit SDK — Python client for the Pit orchestrator.

Communicates with the Pit SDK server over a socket connection.
On Unix the server uses a Unix domain socket; on Windows it uses TCP on localhost.
The address is read from the PIT_SOCKET environment variable,
which is set automatically by the orchestrator for every task.
"""

import json
import os
import socket


def _connect(addr: str) -> socket.socket:
    """Connect to the SDK server, auto-detecting transport from the address format.

    TCP addresses look like ``127.0.0.1:12345`` (used on Windows).
    Anything else is treated as a Unix domain socket path.
    """
    host, _, port = addr.rpartition(":")
    if host and port.isdigit():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, int(port)))
        return s

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(addr)
    return s


def _request(method: str, params: dict[str, str] | None = None) -> str:
    """Send a JSON request to the SDK server and return the result."""
    sock_addr = os.environ.get("PIT_SOCKET")
    if not sock_addr:
        raise RuntimeError(
            "PIT_SOCKET environment variable not set — "
            "are you running inside a Pit task?"
        )

    payload = json.dumps({"method": method, "params": params or {}}).encode()

    with _connect(sock_addr) as s:
        s.sendall(payload)
        s.shutdown(socket.SHUT_WR)

        chunks: list[bytes] = []
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)

    resp = json.loads(b"".join(chunks))

    if resp.get("error"):
        raise RuntimeError(f"SDK error: {resp['error']}")

    return resp.get("result", "")


def get_secret(key: str) -> str:
    """Retrieve a secret from the Pit secrets store.

    Args:
        key: The secret key to look up. Resolution checks the current
             project's section first, then falls back to [global].

    Returns:
        The secret value as a string.

    Raises:
        RuntimeError: If PIT_SOCKET is not set, the key is not found,
                      or the SDK server returns an error.
    """
    return _request("get_secret", {"key": key})
