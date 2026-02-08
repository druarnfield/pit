"""Pit SDK — Python client for the Pit orchestrator.

Communicates with the Pit SDK server over a Unix socket.
The socket path is read from the PIT_SOCKET environment variable,
which is set automatically by the orchestrator for every task.
"""

import json
import os
import socket


def _request(method: str, params: dict[str, str] | None = None) -> str:
    """Send a JSON request to the SDK server and return the result."""
    sock_path = os.environ.get("PIT_SOCKET")
    if not sock_path:
        raise RuntimeError(
            "PIT_SOCKET environment variable not set — "
            "are you running inside a Pit task?"
        )

    payload = json.dumps({"method": method, "params": params or {}}).encode()

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(sock_path)
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
