"""
Asyncio TCP server exposing the storage engine over RESP.

Supported commands (case-insensitive):

    PING [msg]                      GET key
    SET key value [EX s] [PX ms]    DEL key [key ...]
        [NX|XX]                     EXISTS key [key ...]
    EXPIRE key seconds              TTL key / PTTL key
    PERSIST key                     KEYS pattern
    INCR key / DECR key             INCRBY key n
    APPEND key value                STRLEN key
    DBSIZE                          FLUSHDB
    COMPACT                         INFO
    MULTI / EXEC / DISCARD          SAVE
    SYNC (replication handshake)    QUIT

Transactions: MULTI queues commands on the connection; EXEC runs the
whole queue atomically under the engine lock; DISCARD drops it.

Replication: a follower started with ``--replicaof host port`` connects
to the leader, sends SYNC, receives a full copy of the dataset, then
keeps receiving every subsequent write as it happens.
"""

from __future__ import annotations

import asyncio
import fnmatch
import time

from . import resp
from .storage import StorageEngine

OK = "OK"
WRONG_ARGS = "ERR wrong number of arguments for '{}' command"
NOT_INT = "ERR value is not an integer or out of range"

# Commands that mutate state and therefore must be forwarded to replicas.
WRITE_COMMANDS = {b"SET", b"DEL", b"EXPIRE", b"PERSIST", b"INCR", b"DECR",
                  b"INCRBY", b"APPEND", b"FLUSHDB", b"REPLSET"}


class CommandHandler:
    """Maps RESP command arrays onto storage-engine calls."""

    def __init__(self, engine):
        self.engine = engine
        self.started_at = time.time()
        self.commands_processed = 0
        self.role = "leader"

    def dispatch(self, parts: list) -> resp.RespValue:
        if not parts or not isinstance(parts, list):
            return resp.RespError("ERR protocol error: expected array")
        name = bytes(parts[0]).decode(errors="replace").upper()
        args = parts[1:]
        handler = getattr(self, f"cmd_{name.lower()}", None)
        if handler is None:
            return resp.RespError(f"ERR unknown command '{name}'")
        self.commands_processed += 1
        try:
            return handler(args)
        except Exception as exc:  # noqa: BLE001 — never crash the connection
            return resp.RespError(f"ERR internal error: {exc}")

    # --------------------------------------------------------- commands
    def cmd_ping(self, args):
        return args[0] if args else "PONG"

    def cmd_set(self, args):
        if len(args) < 2:
            return resp.RespError(WRONG_ARGS.format("set"))
        key, value, rest = args[0], args[1], args[2:]
        expiry, nx, xx = 0, False, False
        i = 0
        while i < len(rest):
            opt = bytes(rest[i]).upper()
            if opt in (b"EX", b"PX"):
                if i + 1 >= len(rest):
                    return resp.RespError("ERR syntax error")
                try:
                    n = int(rest[i + 1])
                except ValueError:
                    return resp.RespError(NOT_INT)
                expiry = int(time.time()) + (n if opt == b"EX" else max(1, n // 1000))
                i += 2
            elif opt == b"NX":
                nx, i = True, i + 1
            elif opt == b"XX":
                xx, i = True, i + 1
            else:
                return resp.RespError("ERR syntax error")
        exists = key in self.engine
        if (nx and exists) or (xx and not exists):
            return None
        self.engine.put(key, value, expiry)
        return OK

    def cmd_replset(self, args):
        """Internal: leader -> replica full-sync record (key value expiry)."""
        if len(args) != 3:
            return resp.RespError(WRONG_ARGS.format("replset"))
        try:
            expiry = int(args[2])
        except ValueError:
            return resp.RespError(NOT_INT)
        self.engine.put(args[0], args[1], expiry)
        return OK

    def cmd_get(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("get"))
        return self.engine.get(args[0])

    def cmd_del(self, args):
        if not args:
            return resp.RespError(WRONG_ARGS.format("del"))
        return sum(1 for key in args if self.engine.delete(key))

    def cmd_exists(self, args):
        if not args:
            return resp.RespError(WRONG_ARGS.format("exists"))
        return sum(1 for key in args if key in self.engine)

    def cmd_expire(self, args):
        if len(args) != 2:
            return resp.RespError(WRONG_ARGS.format("expire"))
        value = self.engine.get(args[0])
        if value is None:
            return 0
        try:
            seconds = int(args[1])
        except ValueError:
            return resp.RespError(NOT_INT)
        self.engine.put(args[0], value, int(time.time()) + seconds)
        return 1

    def cmd_persist(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("persist"))
        value = self.engine.get(args[0])
        if value is None or not self.engine.expiry_of(args[0]):
            return 0
        self.engine.put(args[0], value, 0)
        return 1

    def _ttl(self, key: bytes) -> int:
        if self.engine.get(key) is None:
            return -2
        expiry = self.engine.expiry_of(key) or 0
        return -1 if expiry == 0 else max(0, int(expiry - time.time()))

    def cmd_ttl(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("ttl"))
        return self._ttl(args[0])

    def cmd_pttl(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("pttl"))
        ttl = self._ttl(args[0])
        return ttl if ttl < 0 else ttl * 1000

    def cmd_keys(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("keys"))
        pattern = bytes(args[0]).decode(errors="replace")
        return [k for k in self.engine.keys()
                if fnmatch.fnmatchcase(k.decode(errors="replace"), pattern)]

    def _incr_by(self, key: bytes, delta: int):
        current = self.engine.get(key)
        if current is None:
            value = delta
        else:
            try:
                value = int(current) + delta
            except ValueError:
                return resp.RespError(NOT_INT)
        self.engine.put(key, str(value).encode(), self.engine.expiry_of(key) or 0)
        return value

    def cmd_incr(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("incr"))
        return self._incr_by(args[0], 1)

    def cmd_decr(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("decr"))
        return self._incr_by(args[0], -1)

    def cmd_incrby(self, args):
        if len(args) != 2:
            return resp.RespError(WRONG_ARGS.format("incrby"))
        try:
            return self._incr_by(args[0], int(args[1]))
        except ValueError:
            return resp.RespError(NOT_INT)

    def cmd_append(self, args):
        if len(args) != 2:
            return resp.RespError(WRONG_ARGS.format("append"))
        current = self.engine.get(args[0]) or b""
        new = current + args[1]
        self.engine.put(args[0], new, self.engine.expiry_of(args[0]) or 0)
        return len(new)

    def cmd_strlen(self, args):
        if len(args) != 1:
            return resp.RespError(WRONG_ARGS.format("strlen"))
        value = self.engine.get(args[0])
        return 0 if value is None else len(value)

    def cmd_dbsize(self, args):
        return len(self.engine)

    def cmd_flushdb(self, args):
        for key in self.engine.keys():
            self.engine.delete(key)
        return OK

    def cmd_compact(self, args):
        stats = self.engine.compact()
        saved = stats["bytes_before"] - stats["bytes_after"]
        return f"OK merged={stats['files_merged']} reclaimed={saved}B"

    def cmd_save(self, args):
        path = self.engine.dir / f"snapshot-{int(time.time())}.snap"
        self.engine.snapshot(path)
        return f"OK {path}"

    def cmd_info(self, args):
        uptime = int(time.time() - self.started_at)
        lines = [
            "# minikv",
            f"engine:{type(self.engine).__name__}",
            f"role:{self.role}",
            f"uptime_seconds:{uptime}",
            f"commands_processed:{self.commands_processed}",
            f"keys:{len(self.engine)}",
        ]
        return ("\r\n".join(lines) + "\r\n").encode()

    def cmd_quit(self, args):
        return OK


class Server:
    def __init__(self, engine, host: str = "127.0.0.1", port: int = 6479,
                 replicaof: tuple[str, int] | None = None):
        self.engine = engine
        self.host = host
        self.port = port
        self.replicaof = replicaof
        self.handler = CommandHandler(engine)
        if replicaof:
            self.handler.role = f"replica of {replicaof[0]}:{replicaof[1]}"
        self.replicas: list[asyncio.StreamWriter] = []
        self._server: asyncio.AbstractServer | None = None
        self._repl_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        if self.replicaof:
            self._repl_task = asyncio.get_event_loop().create_task(
                self._follow(*self.replicaof)
            )

    async def serve_forever(self) -> None:
        await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._repl_task:
            self._repl_task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    # ----------------------------------------------------- replication
    async def _follow(self, host: str, port: int) -> None:
        """Follower side: pull a full copy, then apply the live stream."""
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(resp.encode([b"SYNC"]))
        await writer.drain()
        parser = resp.Parser()
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                parser.feed(data)
                while True:
                    value = parser.parse()
                    if value is resp.NEED_MORE:
                        break
                    if isinstance(value, list):       # a replicated command
                        self.handler.dispatch(value)
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()

    def _propagate(self, request: list) -> None:
        """Leader side: forward a write command to every live replica."""
        if not self.replicas:
            return
        payload = resp.encode([
            bytes(p) if isinstance(p, (bytes, bytearray)) else str(p).encode()
            for p in request
        ])
        for replica in list(self.replicas):
            try:
                replica.write(payload)
            except Exception:  # noqa: BLE001 — dead replica, drop it
                self.replicas.remove(replica)

    async def _full_sync(self, writer: asyncio.StreamWriter) -> None:
        writer.write(resp.encode("FULLSYNC"))
        for key in self.engine.keys():
            value = self.engine.get(key)
            if value is None:
                continue
            expiry = self.engine.expiry_of(key) or 0
            writer.write(resp.encode(
                [b"REPLSET", key, value, str(expiry).encode()]
            ))
        await writer.drain()
        self.replicas.append(writer)

    # ---------------------------------------------------- client loop
    async def _handle_client(self, reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter) -> None:
        parser = resp.Parser()
        queued: list | None = None  # MULTI queue, None = not in a txn
        is_replica_conn = False
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                parser.feed(data)
                while True:
                    try:
                        request = parser.parse()
                    except (resp.ProtocolError, ValueError):
                        writer.write(resp.encode(
                            resp.RespError("ERR protocol error")))
                        await writer.drain()
                        return
                    if request is resp.NEED_MORE:
                        break

                    name = (bytes(request[0]).upper()
                            if isinstance(request, list) and request else b"")

                    # ---- replication handshake -----------------------
                    if name == b"SYNC":
                        await self._full_sync(writer)
                        is_replica_conn = True
                        continue

                    # ---- transactions --------------------------------
                    if name == b"MULTI":
                        if queued is not None:
                            reply = resp.RespError("ERR MULTI calls can not be nested")
                        else:
                            queued, reply = [], OK
                    elif name == b"EXEC":
                        if queued is None:
                            reply = resp.RespError("ERR EXEC without MULTI")
                        else:
                            with self.engine._lock:   # atomic batch
                                reply = [self.handler.dispatch(q) for q in queued]
                                for q in queued:
                                    if bytes(q[0]).upper() in WRITE_COMMANDS:
                                        self._propagate(q)
                            queued = None
                    elif name == b"DISCARD":
                        if queued is None:
                            reply = resp.RespError("ERR DISCARD without MULTI")
                        else:
                            queued, reply = None, OK
                    elif queued is not None:
                        queued.append(request)
                        reply = "QUEUED"

                    # ---- normal dispatch -----------------------------
                    else:
                        reply = self.handler.dispatch(request)
                        if (name in WRITE_COMMANDS
                                and not isinstance(reply, resp.RespError)):
                            self._propagate(request)

                    writer.write(resp.encode(reply))
                    await writer.drain()
                    if name == b"QUIT":
                        return
        finally:
            if is_replica_conn and writer in self.replicas:
                self.replicas.remove(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass


def main() -> None:  # pragma: no cover
    import argparse

    cli = argparse.ArgumentParser(description="MiniKV server")
    cli.add_argument("--dir", default="./minikv-data")
    cli.add_argument("--host", default="127.0.0.1")
    cli.add_argument("--port", type=int, default=6479)
    cli.add_argument("--engine", choices=["bitcask", "lsm"], default="bitcask",
                     help="storage backend (default: bitcask)")
    cli.add_argument("--replicaof", nargs=2, metavar=("HOST", "PORT"),
                     help="run as a read replica of another MiniKV server")
    args = cli.parse_args()

    if args.engine == "lsm":
        from .lsm import LSMEngine
        engine = LSMEngine(args.dir)
    else:
        engine = StorageEngine(args.dir)

    replicaof = (args.replicaof[0], int(args.replicaof[1])) if args.replicaof else None
    server = Server(engine, args.host, args.port, replicaof=replicaof)
    role = f"replica of {replicaof[0]}:{replicaof[1]}" if replicaof else "leader"
    print(f"MiniKV [{args.engine}] listening on {args.host}:{args.port} "
          f"({role}, data: {args.dir})")
    try:
        asyncio.run(server.serve_forever())
    except KeyboardInterrupt:
        pass
    finally:
        engine.close()


if __name__ == "__main__":  # pragma: no cover
    main()
