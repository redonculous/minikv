"""Tiny synchronous client for MiniKV (or any RESP server)."""

from __future__ import annotations

import socket

from . import resp


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 6479,
                 timeout: float = 5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.parser = resp.Parser()

    def execute(self, *parts: bytes | str | int) -> resp.RespValue:
        encoded = [
            p if isinstance(p, bytes) else str(p).encode() for p in parts
        ]
        self.sock.sendall(resp.encode(encoded))
        while True:
            reply = self.parser.parse()
            if reply is not resp.NEED_MORE:
                return reply
            data = self.sock.recv(64 * 1024)
            if not data:
                raise ConnectionError("server closed connection")
            self.parser.feed(data)

    # Convenience wrappers -------------------------------------------------
    def set(self, key, value, ex: int | None = None):
        args = ["SET", key, value]
        if ex is not None:
            args += ["EX", ex]
        return self.execute(*args)

    def get(self, key):
        return self.execute("GET", key)

    def delete(self, *keys):
        return self.execute("DEL", *keys)

    def close(self):
        try:
            self.execute("QUIT")
        except Exception:  # noqa: BLE001
            pass
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
