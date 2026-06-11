import unittest

from minikv import resp


class TestEncode(unittest.TestCase):
    def test_simple_string(self):
        self.assertEqual(resp.encode("OK"), b"+OK\r\n")

    def test_error(self):
        self.assertEqual(resp.encode(resp.RespError("ERR nope")),
                         b"-ERR nope\r\n")

    def test_integer(self):
        self.assertEqual(resp.encode(42), b":42\r\n")
        self.assertEqual(resp.encode(-1), b":-1\r\n")

    def test_bulk_string(self):
        self.assertEqual(resp.encode(b"hello"), b"$5\r\nhello\r\n")
        self.assertEqual(resp.encode(b""), b"$0\r\n\r\n")

    def test_null(self):
        self.assertEqual(resp.encode(None), b"$-1\r\n")

    def test_array(self):
        self.assertEqual(resp.encode([b"GET", b"key"]),
                         b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")

    def test_nested_array(self):
        self.assertEqual(resp.encode([1, [b"a"]]),
                         b"*2\r\n:1\r\n*1\r\n$1\r\na\r\n")


class TestParser(unittest.TestCase):
    def parse_all(self, data: bytes):
        parser = resp.Parser()
        parser.feed(data)
        out = []
        while True:
            value = parser.parse()
            if value is resp.NEED_MORE:
                return out
            out.append(value)

    def test_roundtrip(self):
        for value in ["OK", 42, None, b"hello", [b"SET", b"k", b"v"],
                      resp.RespError("ERR x"), [], [1, 2, [b"nested"]]]:
            with self.subTest(value=value):
                self.assertEqual(self.parse_all(resp.encode(value)), [value])

    def test_incremental_byte_by_byte(self):
        """Parser must handle a request arriving one byte at a time."""
        message = resp.encode([b"SET", b"key", b"value"])
        parser = resp.Parser()
        results = []
        for i in range(len(message)):
            parser.feed(message[i:i + 1])
            value = parser.parse()
            if value is not resp.NEED_MORE:
                results.append(value)
        self.assertEqual(results, [[b"SET", b"key", b"value"]])

    def test_pipelined_requests(self):
        data = resp.encode([b"PING"]) + resp.encode([b"GET", b"k"])
        self.assertEqual(self.parse_all(data), [[b"PING"], [b"GET", b"k"]])

    def test_null_vs_need_more(self):
        parser = resp.Parser()
        parser.feed(b"$-1\r\n")
        self.assertIsNone(parser.parse())          # real null
        self.assertIs(parser.parse(), resp.NEED_MORE)  # empty buffer

    def test_bulk_string_with_crlf_inside(self):
        payload = b"line1\r\nline2"
        self.assertEqual(self.parse_all(resp.encode(payload)), [payload])

    def test_unknown_prefix_raises(self):
        parser = resp.Parser()
        parser.feed(b"?weird\r\n")
        with self.assertRaises(resp.ProtocolError):
            parser.parse()


if __name__ == "__main__":
    unittest.main()
