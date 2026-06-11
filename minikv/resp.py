"""
RESP (REdis Serialization Protocol) v2 encoder/decoder.

MiniKV speaks real RESP, which means you can talk to it with ``redis-cli``
or any Redis client library. Only the subset needed by the server is
implemented: simple strings, errors, integers, bulk strings and arrays.

https://redis.io/docs/reference/protocol-spec/
"""

from __future__ import annotations

from typing import Optional, Union

CRLF = b"\r\n"


class _NeedMore:
    """Sentinel: the parser needs more bytes before a value is complete."""

    def __repr__(self) -> str:  # pragma: no cover
        return "NEED_MORE"


NEED_MORE = _NeedMore()

RespValue = Union[bytes, int, None, list, "RespError", str]


class RespError:
    """An error reply, e.g. ``-ERR unknown command``."""

    def __init__(self, message: str):
        self.message = message

    def __repr__(self) -> str:  # pragma: no cover
        return f"RespError({self.message!r})"

    def __eq__(self, other) -> bool:
        return isinstance(other, RespError) and other.message == self.message


class ProtocolError(Exception):
    """Raised on malformed input."""


# ----------------------------------------------------------------- encode
def encode(value: RespValue) -> bytes:
    """Encode a Python value as RESP bytes.

    str   -> simple string   (+OK\r\n)
    bytes -> bulk string     ($3\r\nfoo\r\n)
    int   -> integer         (:42\r\n)
    None  -> null bulk       ($-1\r\n)
    list  -> array
    RespError -> error       (-ERR ...\r\n)
    """
    if isinstance(value, RespError):
        return b"-" + value.message.encode() + CRLF
    if isinstance(value, str):
        return b"+" + value.encode() + CRLF
    if isinstance(value, bool):  # bool before int: bool is an int subclass
        return b":" + (b"1" if value else b"0") + CRLF
    if isinstance(value, int):
        return b":" + str(value).encode() + CRLF
    if value is None:
        return b"$-1" + CRLF
    if isinstance(value, bytes):
        return b"$" + str(len(value)).encode() + CRLF + value + CRLF
    if isinstance(value, (list, tuple)):
        out = b"*" + str(len(value)).encode() + CRLF
        return out + b"".join(encode(v) for v in value)
    raise TypeError(f"cannot encode {type(value)!r}")


# ----------------------------------------------------------------- decode
class Parser:
    """Incremental RESP parser. Feed bytes in, pull complete values out."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf.extend(data)

    def parse(self) -> RespValue | _NeedMore:
        """Return one complete value, or ``NEED_MORE`` if bytes are missing.

        A null bulk string / null array decodes to ``None`` — distinct from
        ``NEED_MORE``, so callers can tell "no reply yet" from "null reply".
        """
        result = self._parse_at(0)
        if result is None:
            return NEED_MORE
        value, consumed = result
        del self._buf[:consumed]
        return value

    def _line_end(self, start: int) -> Optional[int]:
        idx = self._buf.find(CRLF, start)
        return None if idx == -1 else idx

    def _parse_at(self, pos: int):
        if pos >= len(self._buf):
            return None
        prefix = self._buf[pos:pos + 1]
        end = self._line_end(pos + 1)
        if end is None:
            return None
        line = bytes(self._buf[pos + 1:end])
        after = end + 2

        if prefix == b"+":
            return line.decode(), after - pos
        if prefix == b"-":
            return RespError(line.decode()), after - pos
        if prefix == b":":
            return int(line), after - pos
        if prefix == b"$":
            length = int(line)
            if length == -1:
                return None, after - pos
            if len(self._buf) < after + length + 2:
                return None  # need more bytes
            value = bytes(self._buf[after:after + length])
            return value, after + length + 2 - pos
        if prefix == b"*":
            count = int(line)
            if count == -1:
                return None, after - pos
            items = []
            cursor = after
            for _ in range(count):
                sub = self._parse_at(cursor)
                if sub is None:
                    return None
                value, consumed = sub
                items.append(value)
                cursor += consumed
            return items, cursor - pos
        raise ProtocolError(f"unknown RESP prefix: {prefix!r}")
