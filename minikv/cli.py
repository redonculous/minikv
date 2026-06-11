"""Interactive REPL for MiniKV: ``python -m minikv.cli``"""

from __future__ import annotations

import shlex
import sys

from . import resp
from .client import Client


def _fmt(value: resp.RespValue, indent: int = 0) -> str:
    pad = "  " * indent
    if isinstance(value, resp.RespError):
        return f"{pad}(error) {value.message}"
    if value is None:
        return f"{pad}(nil)"
    if isinstance(value, bytes):
        return f'{pad}"{value.decode(errors="replace")}"'
    if isinstance(value, int):
        return f"{pad}(integer) {value}"
    if isinstance(value, list):
        if not value:
            return f"{pad}(empty array)"
        return "\n".join(
            f"{pad}{i + 1}) {_fmt(v).lstrip()}" for i, v in enumerate(value)
        )
    return f"{pad}{value}"


def main() -> None:
    import argparse

    cli = argparse.ArgumentParser(description="MiniKV interactive client")
    cli.add_argument("--host", default="127.0.0.1")
    cli.add_argument("--port", type=int, default=6479)
    args = cli.parse_args()

    try:
        client = Client(args.host, args.port)
    except OSError as exc:
        print(f"could not connect to {args.host}:{args.port}: {exc}")
        sys.exit(1)

    print(f"minikv {args.host}:{args.port} — type QUIT to exit")
    while True:
        try:
            line = input(f"{args.host}:{args.port}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        parts = shlex.split(line)
        reply = client.execute(*parts)
        print(_fmt(reply))
        if parts[0].upper() == "QUIT":
            break
    client.close()


if __name__ == "__main__":  # pragma: no cover
    main()
