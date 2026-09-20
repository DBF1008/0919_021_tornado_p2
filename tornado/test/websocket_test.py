import asyncio
import contextlib
import datetime
import functools
import socket
import traceback
import typing
import unittest

from tornado import gen
from tornado.concurrent import Future
from tornado.httpclient import HTTPError, HTTPRequest
from tornado.iostream import IOStream
from tornado.locks import Event
from tornado.log import app_log, gen_log
from tornado.netutil import Resolver
from tornado.simple_httpclient import SimpleAsyncHTTPClient
from tornado.template import DictLoader
from tornado.test.util import abstract_base_test, ignore_deprecation
from tornado.testing import (
    AsyncHTTPTestCase,
    AsyncTestCase,
    ExpectLog,
    bind_unused_port,
    gen_test,
)
from tornado.web import Application, RequestHandler

try:
    import tornado.websocket  # noqa: F401
    from tornado.util import _websocket_mask_python
except ImportError:
    # The unittest module presents misleading errors on ImportError
    # (it acts as if websocket_test could not be found, hiding the underlying
    # error).  If we get an ImportError here (which could happen due to
    # TORNADO_EXTENSION=1), print some extra information before failing.
    traceback.print_exc()
    raise

from tornado.websocket import (
    WebSocketClosedError,
    WebSocketError,
    WebSocketHandler,
    WebSocketProtocol13,
    _PerMessageDeflateCompressor,
    _PerMessageDeflateDecompressor,
    _SlidingWindowRateLimiter,
    _WebSocketParams,
    websocket_connect,
)

try:
    from tornado import speedups
except ImportError:
    speedups = None  # type: ignore


class TestWebSocketHandler(WebSocketHandler):
    """Base class for testing handlers that exposes the on_close event.

    This allows for tests to see the close code and reason on the
    server side.

    """

    def initialize(self, close_future=None, compression_options=None):
        self.close_future = close_future
        self.compression_options = compression_options

    def get_compression_options(self):
        return self.compression_options

    def on_close(self):
        if self.close_future is not None:
            self.close_future.set_result((self.close_code, self.close_reason))


class EchoHandler(TestWebSocketHandler):
    @gen.coroutine
    def on_message(self, message):
        try:
            yield self.write_message(message, isinstance(message, bytes))
        except asyncio.CancelledError:
            pass
        except WebSocketClosedError:
            pass


class ErrorInOnMessageHandler(TestWebSocketHandler):
    def on_message(self, message):
        1 / 0


class HeaderHandler(TestWebSocketHandler):
    def open(self):
        methods_to_test = [
            functools.partial(self.write, "This should not work"),
            functools.partial(self.redirect, "http://localhost/elsewhere"),
            functools.partial(self.set_header, "X-Test", ""),
            functools.partial(self.set_cookie, "Chocolate", "Chip"),
            functools.partial(self.set_status, 503),
            self.flush,
            self.finish,
        ]
        for method in methods_to_test:
            try:
                # In a websocket context, many RequestHandler methods
                # raise RuntimeErrors.
                method()  # type: ignore
                raise Exception("did not get expected exception")
            except RuntimeError:
                pass
        self.write_message(self.request.headers.get("X-Test", ""))


class HeaderEchoHandler(TestWebSocketHandler):
    def set_default_headers(self):
        self.set_header("X-Extra-Response-Header", "Extra-Response-Value")

    def prepare(self):
        for k, v in self.request.headers.get_all():
            if k.lower().startswith("x-test"):
                self.set_header(k, v)


class NonWebSocketHandler(RequestHandler):
    def get(self):
        self.write("ok")


class RedirectHandler(RequestHandler):
    def get(self):
        self.redirect("/echo")


class CloseReasonHandler(TestWebSocketHandler):
    def open(self):
        self.on_close_called = False
        self.close(1001, "goodbye")


class AsyncPrepareHandler(TestWebSocketHandler):
    @gen.coroutine
    def prepare(self):
        yield gen.moment

    def on_message(self, message):
        self.write_message(message)


class PathArgsHandler(TestWebSocketHandler):
    def open(self, arg):
        self.write_message(arg)


class CoroutineOnMessageHandler(TestWebSocketHandler):
    def initialize(self, **kwargs):  # type: ignore[override]
        super().initialize(**kwargs)
        self.sleeping = 0

    @gen.coroutine
    def on_message(self, message):
        if self.sleeping > 0:
            self.write_message("another coroutine is already sleeping")
        self.sleeping += 1
        yield gen.sleep(0.01)
        self.sleeping -= 1
        self.write_message(message)


class RenderMessageHandler(TestWebSocketHandler):
    def on_message(self, message):
        self.write_message(self.render_string("message.html", message=message))


class SubprotocolHandler(TestWebSocketHandler):
    def initialize(self, **kwargs):  # type: ignore[override]
        super().initialize(**kwargs)
        self.select_subprotocol_called = False

    def select_subprotocol(self, subprotocols):
        if self.select_subprotocol_called:
            raise Exception("select_subprotocol called twice")
        self.select_subprotocol_called = True
        if "goodproto" in subprotocols:
            return "goodproto"
        return None

    def open(self):
        if not self.select_subprotocol_called:
            raise Exception("select_subprotocol not called")
        self.write_message("subprotocol=%s" % self.selected_subprotocol)


class OpenCoroutineHandler(TestWebSocketHandler):
    def initialize(self, test, **kwargs):  # type: ignore[override]
        super().initialize(**kwargs)
        self.test = test
        self.open_finished = False

    @gen.coroutine
    def open(self):
        yield self.test.message_sent.wait()
        yield gen.sleep(0.010)
        self.open_finished = True

    def on_message(self, message):
        if not self.open_finished:
            raise Exception("on_message called before open finished")
        self.write_message("ok")


class ErrorInOpenHandler(TestWebSocketHandler):
    def open(self):
        raise Exception("boom")


class ErrorInAsyncOpenHandler(TestWebSocketHandler):
    async def open(self):
        await asyncio.sleep(0)
        raise Exception("boom")


class NoDelayHandler(TestWebSocketHandler):
    def open(self):
        self.set_nodelay(True)
        self.write_message("hello")


class WebSocketBaseTestCase(AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        self.conns_to_close = []

    def tearDown(self):
        for conn in self.conns_to_close:
            conn.close()
        super().tearDown()

    @gen.coroutine
    def ws_connect(self, path, **kwargs):
        ws = yield websocket_connect(
            "ws://127.0.0.1:%d%s" % (self.get_http_port(), path), **kwargs
        )
        self.conns_to_close.append(ws)
        raise gen.Return(ws)


class WebSocketTest(WebSocketBaseTestCase):
    def get_app(self):
        self.close_future: Future[None] = Future()
        return Application(
            [
                ("/echo", EchoHandler, dict(close_future=self.close_future)),
                ("/non_ws", NonWebSocketHandler),
                ("/redirect", RedirectHandler),
                ("/header", HeaderHandler, dict(close_future=self.close_future)),
                (
                    "/header_echo",
                    HeaderEchoHandler,
                    dict(close_future=self.close_future),
                ),
                (
                    "/close_reason",
                    CloseReasonHandler,
                    dict(close_future=self.close_future),
                ),
                (
                    "/error_in_on_message",
                    ErrorInOnMessageHandler,
                    dict(close_future=self.close_future),
                ),
                (
                    "/async_prepare",
                    AsyncPrepareHandler,
                    dict(close_future=self.close_future),
                ),
                (
                    "/path_args/(.*)",
                    PathArgsHandler,
                    dict(close_future=self.close_future),
                ),
                (
                    "/coroutine",
                    CoroutineOnMessageHandler,
                    dict(close_future=self.close_future),
                ),
                ("/render", RenderMessageHandler, dict(close_future=self.close_future)),
                (
                    "/subprotocol",
                    SubprotocolHandler,
                    dict(close_future=self.close_future),
                ),
                (
                    "/open_coroutine",
                    OpenCoroutineHandler,
                    dict(close_future=self.close_future, test=self),
                ),
                ("/error_in_open", ErrorInOpenHandler),
                ("/error_in_async_open", ErrorInAsyncOpenHandler),
                ("/nodelay", NoDelayHandler),
            ],
            template_loader=DictLoader({"message.html": "<b>{{ message }}</b>"}),
        )

    def get_http_client(self):
        # These tests require HTTP/1; force the use of SimpleAsyncHTTPClient.
        return SimpleAsyncHTTPClient()

    def tearDown(self):
        super().tearDown()
        RequestHandler._template_loaders.clear()

    def test_http_request(self):
        # WS server, HTTP client.
        response = self.fetch("/echo")
        self.assertEqual(response.code, 400)

    def test_missing_websocket_key(self):
        response = self.fetch(
            "/echo",
            headers={
                "Connection": "Upgrade",
                "Upgrade": "WebSocket",
                "Sec-WebSocket-Version": "13",
            },
        )
        self.assertEqual(response.code, 400)

    def test_bad_websocket_version(self):
        response = self.fetch(
            "/echo",
            headers={
                "Connection": "Upgrade",
                "Upgrade": "WebSocket",
                "Sec-WebSocket-Version": "12",
            },
        )
        self.assertEqual(response.code, 426)

    @gen_test
    def test_websocket_gen(self):
        ws = yield self.ws_connect("/echo")
        yield ws.write_message("hello")
        response = yield ws.read_message()
        self.assertEqual(response, "hello")

    def test_websocket_callbacks(self):
        with ignore_deprecation():
            websocket_connect(
                "ws://127.0.0.1:%d/echo" % self.get_http_port(), callback=self.stop
            )
        ws = self.wait().result()
        ws.write_message("hello")
        ws.read_message(self.stop)
        response = self.wait().result()
        self.assertEqual(response, "hello")
        self.close_future.add_done_callback(lambda f: self.stop())
        ws.close()
        self.wait()

    @gen_test
    def test_binary_message(self):
        ws = yield self.ws_connect("/echo")
        ws.write_message(b"hello \xe9", binary=True)
        response = yield ws.read_message()
        self.assertEqual(response, b"hello \xe9")

    @gen_test
    def test_unicode_message(self):
        ws = yield self.ws_connect("/echo")
        ws.write_message("hello \u00e9")
        response = yield ws.read_message()
        self.assertEqual(response, "hello \u00e9")

    @gen_test
    def test_error_in_closed_client_write_message(self):
        ws = yield self.ws_connect("/echo")
        ws.close()
        with self.assertRaises(WebSocketClosedError):
            ws.write_message("hello \u00e9")

    @gen_test
    def test_render_message(self):
        ws = yield self.ws_connect("/render")
        ws.write_message("hello")
        response = yield ws.read_message()
        self.assertEqual(response, "<b>hello</b>")

    @gen_test
    def test_error_in_on_message(self):
        ws = yield self.ws_connect("/error_in_on_message")
        ws.write_message("hello")
        with ExpectLog(app_log, "Uncaught exception"):
            response = yield ws.read_message()
        self.assertIsNone(response)

    @gen_test
    def test_websocket_http_fail(self):
        with self.assertRaises(HTTPError) as cm:
            yield self.ws_connect("/notfound")
        self.assertEqual(cm.exception.code, 404)

    @gen_test
    def test_websocket_http_success(self):
        with self.assertRaises(WebSocketError):
            yield self.ws_connect("/non_ws")

    @gen_test
    def test_websocket_http_redirect(self):
        with self.assertRaises(HTTPError):
            yield self.ws_connect("/redirect")

    @gen_test
    def test_websocket_network_fail(self):
        sock, port = bind_unused_port()
        sock.close()
        with self.assertRaises(IOError):
            with ExpectLog(gen_log, ".*", required=False):
                yield websocket_connect(
                    "ws://127.0.0.1:%d/" % port, connect_timeout=3600
                )

    @gen_test
    def test_websocket_close_buffered_data(self):
        with contextlib.closing(
            (yield websocket_connect("ws://127.0.0.1:%d/echo" % self.get_http_port()))
        ) as ws:
            ws.write_message("hello")
            ws.write_message("world")
            # Close the underlying stream.
            ws.stream.close()

    @gen_test
    def test_websocket_headers(self):
        # Ensure that arbitrary headers can be passed through websocket_connect.
        with contextlib.closing(
            (
                yield websocket_connect(
                    HTTPRequest(
                        "ws://127.0.0.1:%d/header" % self.get_http_port(),
                        headers={"X-Test": "hello"},
                    )
                )
            )
        ) as ws:
            response = yield ws.read_message()
            self.assertEqual(response, "hello")

    @gen_test
    def test_websocket_header_echo(self):
        # Ensure that headers can be returned in the response.
        # Specifically, that arbitrary headers passed through websocket_connect
        # can be returned.
        with contextlib.closing(
            (
                yield websocket_connect(
                    HTTPRequest(
                        "ws://127.0.0.1:%d/header_echo" % self.get_http_port(),
                        headers={"X-Test-Hello": "hello"},
                    )
                )
            )
        ) as ws:
            self.assertEqual(ws.headers.get("X-Test-Hello"), "hello")
            self.assertEqual(
                ws.headers.get("X-Extra-Response-Header"), "Extra-Response-Value"
            )

    @gen_test
    def test_server_close_reason(self):
        ws = yield self.ws_connect("/close_reason")
        msg = yield ws.read_message()
        # A message of None means the other side closed the connection.
        self.assertIs(msg, None)
        self.assertEqual(ws.close_code, 1001)
        self.assertEqual(ws.close_reason, "goodbye")
        # The on_close callback is called no matter which side closed.
        code, reason = yield self.close_future
        # The client echoed the close code it received to the server,
        # so the server's close code (returned via close_future) is
        # the same.
        self.assertEqual(code, 1001)

    @gen_test
    def test_client_close_reason(self):
        ws = yield self.ws_connect("/echo")
        ws.close(1001, "goodbye")
        code, reason = yield self.close_future
        self.assertEqual(code, 1001)
        self.assertEqual(reason, "goodbye")

    @gen_test
    def test_write_after_close(self):
        ws = yield self.ws_connect("/close_reason")
        msg = yield ws.read_message()
        self.assertIs(msg, None)
        with self.assertRaises(WebSocketClosedError):
            ws.write_message("hello")

    @gen_test
    def test_async_prepare(self):
        # Previously, an async prepare method triggered a bug that would
        # result in a timeout on test shutdown (and a memory leak).
        ws = yield self.ws_connect("/async_prepare")
        ws.write_message("hello")
        res = yield ws.read_message()
        self.assertEqual(res, "hello")

    @gen_test
    def test_path_args(self):
        ws = yield self.ws_connect("/path_args/hello")
        res = yield ws.read_message()
        self.assertEqual(res, "hello")

    @gen_test
    def test_coroutine(self):
        ws = yield self.ws_connect("/coroutine")
        # Send both messages immediately, coroutine must process one at a time.
        yield ws.write_message("hello1")
        yield ws.write_message("hello2")
        res = yield ws.read_message()
        self.assertEqual(res, "hello1")
        res = yield ws.read_message()
        self.assertEqual(res, "hello2")

    @gen_test
    def test_check_origin_valid_no_path(self):
        port = self.get_http_port()

        url = "ws://127.0.0.1:%d/echo" % port
        headers = {"Origin": "http://127.0.0.1:%d" % port}

        with contextlib.closing(
            (yield websocket_connect(HTTPRequest(url, headers=headers)))
        ) as ws:
            ws.write_message("hello")
            response = yield ws.read_message()
            self.assertEqual(response, "hello")

    @gen_test
    def test_check_origin_valid_with_path(self):
        port = self.get_http_port()

        url = "ws://127.0.0.1:%d/echo" % port
        headers = {"Origin": "http://127.0.0.1:%d/something" % port}

        with contextlib.closing(
            (yield websocket_connect(HTTPRequest(url, headers=headers)))
        ) as ws:
            ws.write_message("hello")
            response = yield ws.read_message()
            self.assertEqual(response, "hello")

    @gen_test
    def test_check_origin_invalid_partial_url(self):
        port = self.get_http_port()

        url = "ws://127.0.0.1:%d/echo" % port
        headers = {"Origin": "127.0.0.1:%d" % port}

        with self.assertRaises(HTTPError) as cm:
            yield websocket_connect(HTTPRequest(url, headers=headers))
        self.assertEqual(cm.exception.code, 403)

    @gen_test
    def test_check_origin_invalid(self):
        port = self.get_http_port()

        url = "ws://127.0.0.1:%d/echo" % port
        # Host is 127.0.0.1, which should not be accessible from some other
        # domain
        headers = {"Origin": "http://somewhereelse.com"}

        with self.assertRaises(HTTPError) as cm:
            yield websocket_connect(HTTPRequest(url, headers=headers))

        self.assertEqual(cm.exception.code, 403)

    @gen_test
    def test_check_origin_invalid_subdomains(self):
        port = self.get_http_port()

        # CaresResolver may return ipv6-only results for localhost, but our
        # server is only running on ipv4. Test for this edge case and skip
        # the test if it happens.
        addrinfo = yield Resolver().resolve("localhost", port)
        families = {addr[0] for addr in addrinfo}
        if socket.AF_INET not in families:
            self.skipTest("localhost does not resolve to ipv4")
            return

        url = "ws://localhost:%d/echo" % port
        # Subdomains should be disallowed by default.  If we could pass a
        # resolver to websocket_connect we could test sibling domains as well.
        headers = {"Origin": "http://subtenant.localhost"}

        with self.assertRaises(HTTPError) as cm:
            yield websocket_connect(HTTPRequest(url, headers=headers))

        self.assertEqual(cm.exception.code, 403)

    @gen_test
    def test_subprotocols(self):
        ws = yield self.ws_connect(
            "/subprotocol", subprotocols=["badproto", "goodproto"]
        )
        self.assertEqual(ws.selected_subprotocol, "goodproto")
        res = yield ws.read_message()
        self.assertEqual(res, "subprotocol=goodproto")

    @gen_test
    def test_subprotocols_not_offered(self):
        ws = yield self.ws_connect("/subprotocol")
        self.assertIs(ws.selected_subprotocol, None)
        res = yield ws.read_message()
        self.assertEqual(res, "subprotocol=None")

    @gen_test
    def test_open_coroutine(self):
        self.message_sent = Event()
        ws = yield self.ws_connect("/open_coroutine")
        yield ws.write_message("hello")
        self.message_sent.set()
        res = yield ws.read_message()
        self.assertEqual(res, "ok")

    @gen_test
    def test_error_in_open(self):
        with ExpectLog(app_log, "Uncaught exception"):
            ws = yield self.ws_connect("/error_in_open")
            res = yield ws.read_message()
        self.assertIsNone(res)

    @gen_test
    def test_error_in_async_open(self):
        with ExpectLog(app_log, "Uncaught exception"):
            ws = yield self.ws_connect("/error_in_async_open")
            res = yield ws.read_message()
        self.assertIsNone(res)

    @gen_test
    def test_nodelay(self):
        ws = yield self.ws_connect("/nodelay")
        res = yield ws.read_message()
        self.assertEqual(res, "hello")


class NativeCoroutineOnMessageHandler(TestWebSocketHandler):
    def initialize(self, **kwargs):  # type: ignore[override]
        super().initialize(**kwargs)
        self.sleeping = 0

    async def on_message(self, message):
        if self.sleeping > 0:
            self.write_message("another coroutine is already sleeping")
        self.sleeping += 1
        await gen.sleep(0.01)
        self.sleeping -= 1
        self.write_message(message)


class WebSocketNativeCoroutineTest(WebSocketBaseTestCase):
    def get_app(self):
        return Application([("/native", NativeCoroutineOnMessageHandler)])

    @gen_test
    def test_native_coroutine(self):
        ws = yield self.ws_connect("/native")
        # Send both messages immediately, coroutine must process one at a time.
        yield ws.write_message("hello1")
        yield ws.write_message("hello2")
        res = yield ws.read_message()
        self.assertEqual(res, "hello1")
        res = yield ws.read_message()
        self.assertEqual(res, "hello2")


@abstract_base_test
class CompressionTestMixin(WebSocketBaseTestCase):
    MESSAGE = "Hello world. Testing 123 123"

    def get_app(self):
        class LimitedHandler(TestWebSocketHandler):
            @property
            def max_message_size(self):
                return 1024

            def on_message(self, message):
                self.write_message(str(len(message)))

        return Application(
            [
                (
                    "/echo",
                    EchoHandler,
                    dict(compression_options=self.get_server_compression_options()),
                ),
                (
                    "/limited",
                    LimitedHandler,
                    dict(compression_options=self.get_server_compression_options()),
                ),
            ]
        )

    def get_server_compression_options(self):
        return None

    def get_client_compression_options(self):
        return None

    def verify_wire_bytes(self, bytes_in: int, bytes_out: int) -> None:
        raise NotImplementedError()

    @gen_test
    def test_message_sizes(self):
        ws = yield self.ws_connect(
            "/echo", compression_options=self.get_client_compression_options()
        )
        # Send the same message three times so we can measure the
        # effect of the context_takeover options.
        for i in range(3):
            ws.write_message(self.MESSAGE)
            response = yield ws.read_message()
            self.assertEqual(response, self.MESSAGE)
        self.assertEqual(ws.protocol._message_bytes_out, len(self.MESSAGE) * 3)
        self.assertEqual(ws.protocol._message_bytes_in, len(self.MESSAGE) * 3)
        self.verify_wire_bytes(ws.protocol._wire_bytes_in, ws.protocol._wire_bytes_out)

    @gen_test
    def test_size_limit(self):
        ws = yield self.ws_connect(
            "/limited", compression_options=self.get_client_compression_options()
        )
        # Small messages pass through.
        ws.write_message("a" * 128)
        response = yield ws.read_message()
        self.assertEqual(response, "128")
        # This message is too big after decompression, but it compresses
        # down to a size that will pass the initial checks.
        ws.write_message("a" * 2048)
        response = yield ws.read_message()
        self.assertIsNone(response)


@abstract_base_test
class UncompressedTestMixin(CompressionTestMixin):
    """Specialization of CompressionTestMixin when we expect no compression."""

    def verify_wire_bytes(self, bytes_in, bytes_out):
        # Bytes out includes the 4-byte mask key per message.
        self.assertEqual(bytes_out, 3 * (len(self.MESSAGE) + 6))
        self.assertEqual(bytes_in, 3 * (len(self.MESSAGE) + 2))


class NoCompressionTest(UncompressedTestMixin):
    pass


# If only one side tries to compress, the extension is not negotiated.
class ServerOnlyCompressionTest(UncompressedTestMixin):
    def get_server_compression_options(self):
        return {}


class ClientOnlyCompressionTest(UncompressedTestMixin):
    def get_client_compression_options(self):
        return {}


class DefaultCompressionTest(CompressionTestMixin):
    def get_server_compression_options(self):
        return {}

    def get_client_compression_options(self):
        return {}

    def verify_wire_bytes(self, bytes_in, bytes_out):
        self.assertLess(bytes_out, 3 * (len(self.MESSAGE) + 6))
        self.assertLess(bytes_in, 3 * (len(self.MESSAGE) + 2))
        # Bytes out includes the 4 bytes mask key per message.
        self.assertEqual(bytes_out, bytes_in + 12)


class _DummyDelegate:
    """Minimal _WebSocketDelegate implementation for protocol-level tests."""

    def on_ws_connection_close(self, close_code=None, close_reason=None):
        pass

    def on_message(self, message):
        pass

    def on_ping(self, data):
        pass

    def on_pong(self, data):
        pass

    def log_exception(self, typ, value, tb):
        pass


def _make_protocol(compression_options=None):
    params = _WebSocketParams(compression_options=compression_options)
    return WebSocketProtocol13(_DummyDelegate(), False, params)


class _FakeStream:
    """Synchronous stand-in for an IOStream in protocol-level tests."""

    def __init__(self, io_loop):
        self.io_loop = io_loop
        self.close_called = False
        self.written = []

    def closed(self):
        return False

    def close(self):
        self.close_called = True

    def write(self, data):
        self.written.append(data)
        future = Future()
        future.set_result(None)
        return future

    def set_nodelay(self, value):
        pass


class PerMessageDeflateNegotiationTest(unittest.TestCase):
    """Unit tests for RFC 7692 parameter negotiation (no I/O involved)."""

    def negotiate(self, offered, options):
        return _make_protocol()._negotiate_permessage_deflate(offered, options)

    def test_empty_offer_empty_config(self):
        self.assertEqual(self.negotiate({}, {}), {})

    def test_offered_parameters_are_echoed(self):
        agreed = self.negotiate(
            {"server_no_context_takeover": None, "client_max_window_bits": "12"},
            {},
        )
        self.assertEqual(
            agreed,
            {
                "server_no_context_takeover": None,
                "client_max_window_bits": "12",
            },
        )

    def test_config_adds_no_context_takeover(self):
        # The server may include either no_context_takeover parameter
        # even if the client did not offer it (RFC 7692 7.1.1).
        agreed = self.negotiate(
            {},
            {"server_no_context_takeover": True, "client_no_context_takeover": True},
        )
        self.assertEqual(
            agreed,
            {
                "server_no_context_takeover": None,
                "client_no_context_takeover": None,
            },
        )

    def test_server_max_window_bits_from_config(self):
        # The server may include server_max_window_bits even if the
        # client did not offer it (RFC 7692 7.1.2.1).
        self.assertEqual(
            self.negotiate({}, {"server_max_window_bits": 10}),
            {"server_max_window_bits": "10"},
        )

    def test_server_max_window_bits_min_of_offer_and_config(self):
        self.assertEqual(
            self.negotiate(
                {"server_max_window_bits": "12"}, {"server_max_window_bits": 10}
            ),
            {"server_max_window_bits": "10"},
        )
        self.assertEqual(
            self.negotiate(
                {"server_max_window_bits": "9"}, {"server_max_window_bits": 10}
            ),
            {"server_max_window_bits": "9"},
        )

    def test_client_max_window_bits_requires_offer(self):
        # client_max_window_bits must not appear in the response unless
        # the client offered it (RFC 7692 7.1.2.2).
        self.assertEqual(self.negotiate({}, {"client_max_window_bits": 10}), {})

    def test_client_max_window_bits_valueless_offer_uses_config(self):
        self.assertEqual(
            self.negotiate(
                {"client_max_window_bits": None}, {"client_max_window_bits": 10}
            ),
            {"client_max_window_bits": "10"},
        )

    def test_client_max_window_bits_valueless_offer_no_config(self):
        # A valueless offer is not echoed back without a configured value.
        self.assertEqual(self.negotiate({"client_max_window_bits": None}, {}), {})

    def test_client_max_window_bits_min_of_offer_and_config(self):
        self.assertEqual(
            self.negotiate(
                {"client_max_window_bits": "9"}, {"client_max_window_bits": 10}
            ),
            {"client_max_window_bits": "9"},
        )

    def test_unknown_parameter_declines_offer(self):
        self.assertIsNone(self.negotiate({"x_unknown": "1"}, {}))

    def test_valueless_server_max_window_bits_declines_offer(self):
        # server_max_window_bits in an offer must carry a value.
        self.assertIsNone(self.negotiate({"server_max_window_bits": None}, {}))

    def test_invalid_window_bits_decline_offer(self):
        self.assertIsNone(self.negotiate({"server_max_window_bits": "16"}, {}))
        self.assertIsNone(self.negotiate({"server_max_window_bits": "7"}, {}))
        self.assertIsNone(self.negotiate({"server_max_window_bits": "abc"}, {}))
        self.assertIsNone(self.negotiate({"client_max_window_bits": "99"}, {}))

    def test_invalid_config_window_bits_raises(self):
        with self.assertRaises(ValueError):
            self.negotiate({}, {"server_max_window_bits": 99})
        with self.assertRaises(ValueError):
            self.negotiate(
                {"client_max_window_bits": None}, {"client_max_window_bits": 4}
            )


class SlidingWindowRateLimiterTest(unittest.TestCase):
    def make_limiter(self, max_attempts=2, window=10.0):
        clock_values = [1000.0]

        def clock():
            return clock_values[0]

        limiter = _SlidingWindowRateLimiter(max_attempts, window, clock=clock)
        return limiter, clock_values

    def test_allows_up_to_limit(self):
        limiter, _ = self.make_limiter()
        self.assertTrue(limiter.allow("1.2.3.4"))
        self.assertTrue(limiter.allow("1.2.3.4"))
        self.assertFalse(limiter.allow("1.2.3.4"))
        self.assertFalse(limiter.allow("1.2.3.4"))

    def test_keys_are_independent(self):
        limiter, _ = self.make_limiter(max_attempts=1)
        self.assertTrue(limiter.allow("1.1.1.1"))
        self.assertTrue(limiter.allow("2.2.2.2"))
        self.assertFalse(limiter.allow("1.1.1.1"))
        self.assertFalse(limiter.allow("2.2.2.2"))

    def test_window_expiry_allows_new_attempts(self):
        limiter, clock = self.make_limiter(max_attempts=1, window=10.0)
        self.assertTrue(limiter.allow("1.2.3.4"))
        self.assertFalse(limiter.allow("1.2.3.4"))
        clock[0] += 11.0
        self.assertTrue(limiter.allow("1.2.3.4"))

    def test_idle_keys_are_swept(self):
        # Keys that have been idle for a full window are dropped by the
        # periodic sweep so the dict cannot grow without bound.
        limiter, clock = self.make_limiter(max_attempts=1, window=10.0)
        limiter.allow("1.1.1.1")
        limiter.allow("2.2.2.2")
        self.assertEqual(len(limiter), 2)
        clock[0] += 11.0
        # The next attempt triggers the full sweep.
        limiter.allow("3.3.3.3")
        self.assertEqual(sorted(limiter._events.keys()), ["3.3.3.3"])

    def test_invalid_configuration(self):
        with self.assertRaises(ValueError):
            _SlidingWindowRateLimiter(0, 10.0)
        with self.assertRaises(ValueError):
            _SlidingWindowRateLimiter(1, 0.0)


class CompressionContextReleaseTest(AsyncTestCase):
    """The zlib contexts must be released as soon as the connection
    starts closing, without waiting for the peer's close frame."""

    def make_protocol(self):
        protocol = _make_protocol(compression_options={})
        protocol._create_compressors("server", {}, {})
        protocol.stream = _FakeStream(self.io_loop)
        return protocol

    def test_close_releases_contexts_immediately(self):
        protocol = self.make_protocol()
        self.assertIsNotNone(protocol._compressor)
        self.assertIsNotNone(protocol._decompressor)
        protocol.close(1000, "bye")
        # Released synchronously: the peer has not sent (and may never
        # send) its close frame, and on_close has not run.
        self.assertIsNone(protocol._compressor)
        self.assertIsNone(protocol._decompressor)

    def test_abort_releases_contexts(self):
        protocol = self.make_protocol()
        protocol._abort()
        self.assertIsNone(protocol._compressor)
        self.assertIsNone(protocol._decompressor)
        self.assertTrue(protocol.stream.close_called)

    def test_compressor_close_releases_zlib_object(self):
        compressor = _PerMessageDeflateCompressor(persistent=True, max_wbits=None)
        self.assertIsNotNone(compressor._compressor)
        compressor.close()
        self.assertIsNone(compressor._compressor)

    def test_decompressor_close_releases_zlib_object(self):
        decompressor = _PerMessageDeflateDecompressor(
            persistent=True, max_wbits=None, max_message_size=1024
        )
        self.assertIsNotNone(decompressor._decompressor)
        decompressor.close()
        self.assertIsNone(decompressor._decompressor)


class CompressionNegotiationIntegrationTest(WebSocketBaseTestCase):
    """End-to-end tests for RFC 7692 parameter negotiation."""

    MESSAGE = "Hello world. Testing 123 123"

    def get_app(self):
        return Application(
            [
                (
                    "/snct",
                    EchoHandler,
                    dict(compression_options={"server_no_context_takeover": True}),
                ),
                (
                    "/cnct",
                    EchoHandler,
                    dict(compression_options={"client_no_context_takeover": True}),
                ),
                (
                    "/smwb",
                    EchoHandler,
                    dict(compression_options={"server_max_window_bits": 10}),
                ),
                (
                    "/cmwb",
                    EchoHandler,
                    dict(compression_options={"client_max_window_bits": 10}),
                ),
                (
                    "/all",
                    EchoHandler,
                    dict(
                        compression_options={
                            "server_no_context_takeover": True,
                            "client_no_context_takeover": True,
                            "server_max_window_bits": 10,
                            "client_max_window_bits": 10,
                        }
                    ),
                ),
                ("/default", EchoHandler, dict(compression_options={})),
            ]
        )

    def get_extensions_header(self, ws):
        return ws.headers.get("Sec-WebSocket-Extensions", "")

    @gen.coroutine
    def echo_roundtrip(self, ws):
        # Send the same message three times to exercise the context
        # takeover (or lack thereof) on both sides.
        for _ in range(3):
            ws.write_message(self.MESSAGE)
            response = yield ws.read_message()
            self.assertEqual(response, self.MESSAGE)

    @gen.coroutine
    def raw_ws_connect(self, path, extensions_header):
        request = HTTPRequest(
            "ws://127.0.0.1:%d%s" % (self.get_http_port(), path),
            headers={"Sec-WebSocket-Extensions": extensions_header},
        )
        ws = yield websocket_connect(request)
        self.conns_to_close.append(ws)
        raise gen.Return(ws)

    @gen_test
    def test_server_no_context_takeover(self):
        ws = yield self.ws_connect("/snct", compression_options={})
        self.assertIn("server_no_context_takeover", self.get_extensions_header(ws))
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_client_no_context_takeover(self):
        ws = yield self.ws_connect("/cnct", compression_options={})
        self.assertIn("client_no_context_takeover", self.get_extensions_header(ws))
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_server_max_window_bits(self):
        ws = yield self.ws_connect("/smwb", compression_options={})
        self.assertIn("server_max_window_bits=10", self.get_extensions_header(ws))
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_client_max_window_bits_from_config(self):
        # The client offers a valueless client_max_window_bits and the
        # server config picks the value.
        ws = yield self.ws_connect("/cmwb", compression_options={})
        self.assertIn("client_max_window_bits=10", self.get_extensions_header(ws))
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_client_max_window_bits_min_wins(self):
        # The client offers 9, the server config is 10: 9 wins.
        ws = yield self.ws_connect(
            "/cmwb", compression_options={"client_max_window_bits": 9}
        )
        self.assertIn("client_max_window_bits=9", self.get_extensions_header(ws))
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_all_options(self):
        ws = yield self.ws_connect("/all", compression_options={})
        header = self.get_extensions_header(ws)
        self.assertIn("server_no_context_takeover", header)
        self.assertIn("client_no_context_takeover", header)
        self.assertIn("server_max_window_bits=10", header)
        self.assertIn("client_max_window_bits=10", header)
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_client_offer_includes_configured_params(self):
        # The client sends its configured parameters in the offer and
        # the server (with an empty config) accepts them.
        ws = yield self.ws_connect(
            "/default",
            compression_options={
                "server_no_context_takeover": True,
                "client_no_context_takeover": True,
                "server_max_window_bits": 12,
            },
        )
        header = self.get_extensions_header(ws)
        self.assertIn("server_no_context_takeover", header)
        self.assertIn("client_no_context_takeover", header)
        self.assertIn("server_max_window_bits=12", header)
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_unknown_parameter_offer_declined(self):
        ws = yield self.raw_ws_connect(
            "/default", "permessage-deflate; unknown_param=1"
        )
        # The connection still succeeds, but compression is not used.
        self.assertNotIn("Sec-WebSocket-Extensions", ws.headers)
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_invalid_window_bits_offer_declined(self):
        ws = yield self.raw_ws_connect(
            "/default", "permessage-deflate; server_max_window_bits=99"
        )
        self.assertNotIn("Sec-WebSocket-Extensions", ws.headers)
        yield self.echo_roundtrip(ws)

    @gen_test
    def test_valueless_server_max_window_bits_offer_declined(self):
        ws = yield self.raw_ws_connect(
            "/default", "permessage-deflate; server_max_window_bits"
        )
        self.assertNotIn("Sec-WebSocket-Extensions", ws.headers)
        yield self.echo_roundtrip(ws)


class WebSocketRateLimitTest(WebSocketBaseTestCase):
    def get_app(self):
        return Application(
            [("/echo", EchoHandler)],
            websocket_rate_limit=2,
            websocket_rate_limit_window=60.0,
        )

    @gen_test
    def test_handshakes_within_limit_succeed(self):
        for _ in range(2):
            ws = yield self.ws_connect("/echo")
            ws.write_message("hello")
            response = yield ws.read_message()
            self.assertEqual(response, "hello")

    @gen_test
    def test_handshakes_over_limit_get_429(self):
        yield self.ws_connect("/echo")
        yield self.ws_connect("/echo")
        with self.assertRaises(HTTPError) as cm:
            yield self.ws_connect("/echo")
        self.assertEqual(cm.exception.code, 429)

    @gen_test
    def test_rejected_handshake_closes_stream(self):
        yield self.ws_connect("/echo")
        yield self.ws_connect("/echo")
        # A raw handshake attempt gets a 429 response and the server
        # closes the underlying connection immediately (no WebSocket
        # close frame is possible before the upgrade).
        stream = IOStream(socket.socket())
        yield stream.connect(("127.0.0.1", self.get_http_port()))
        yield stream.write(
            b"GET /echo HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Upgrade: websocket\r\n"
            b"Connection: Upgrade\r\n"
            b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            b"Sec-WebSocket-Version: 13\r\n"
            b"\r\n"
        )
        # read_until_close only completes once the server has closed
        # the underlying stream.
        response = yield stream.read_until_close()
        self.assertIn(b"429 Too Many Requests", response)
        self.assertIn(b"Too Many Requests", response)
        stream.close()


class WebSocketRateLimitDisabledTest(WebSocketBaseTestCase):
    def get_app(self):
        return Application([("/echo", EchoHandler)])

    @gen_test
    def test_no_rate_limit_by_default(self):
        for _ in range(5):
            ws = yield self.ws_connect("/echo")
            ws.write_message("hello")
            response = yield ws.read_message()
            self.assertEqual(response, "hello")


class CompressionReleaseIntegrationTest(WebSocketBaseTestCase):
    def get_app(self):
        self.close_future: Future[None] = Future()
        self.server_handlers = []

        class TrackedEchoHandler(TestWebSocketHandler):
            def initialize(handler_self, **kwargs):
                handler_self.compression_options = {}
                handler_self.close_future = self.close_future

            def get_compression_options(handler_self):
                return handler_self.compression_options

            def open(handler_self):
                self.server_handlers.append(handler_self)

            def on_message(handler_self, message):
                handler_self.write_message(message)

            def on_close(handler_self):
                self.close_future.set_result(
                    (handler_self.close_code, handler_self.close_reason)
                )

        return Application([("/echo", TrackedEchoHandler)])

    @gen_test
    def test_server_close_releases_contexts_before_ack(self):
        ws = yield self.ws_connect("/echo", compression_options={})
        handler = self.server_handlers[0]
        protocol = handler.ws_connection
        self.assertIsNotNone(protocol._compressor)
        self.assertIsNotNone(protocol._decompressor)
        handler.close(1000, "bye")
        # The compression contexts are released synchronously when the
        # close handshake starts, without waiting for the client's close
        # frame (which triggers on_close).
        self.assertIsNone(protocol._compressor)
        self.assertIsNone(protocol._decompressor)
        # The close handshake still completes normally.
        code, reason = yield self.close_future
        self.assertEqual(code, 1000)

    @gen_test
    def test_client_close_releases_contexts(self):
        ws = yield self.ws_connect("/echo", compression_options={})
        protocol = ws.protocol
        self.assertIsNotNone(protocol._compressor)
        self.assertIsNotNone(protocol._decompressor)
        ws.close()
        self.assertIsNone(protocol._compressor)
        self.assertIsNone(protocol._decompressor)
        yield self.close_future

    @gen_test
    def test_client_initiated_close_releases_server_contexts(self):
        ws = yield self.ws_connect("/echo", compression_options={})
        handler = self.server_handlers[0]
        protocol = handler.ws_connection
        ws.close(1000, "client done")
        yield self.close_future
        self.assertIsNone(protocol._compressor)
        self.assertIsNone(protocol._decompressor)


@abstract_base_test
class MaskFunctionMixin(unittest.TestCase):
    # Subclasses should define self.mask(mask, data)
    def mask(self, mask: bytes, data: bytes) -> bytes:
        raise NotImplementedError()

    def test_mask(self: typing.Any):
        self.assertEqual(self.mask(b"abcd", b""), b"")
        self.assertEqual(self.mask(b"abcd", b"b"), b"\x03")
        self.assertEqual(self.mask(b"abcd", b"54321"), b"TVPVP")
        self.assertEqual(self.mask(b"ZXCV", b"98765432"), b"c`t`olpd")
        # Include test cases with \x00 bytes (to ensure that the C
        # extension isn't depending on null-terminated strings) and
        # bytes with the high bit set (to smoke out signedness issues).
        self.assertEqual(
            self.mask(b"\x00\x01\x02\x03", b"\xff\xfb\xfd\xfc\xfe\xfa"),
            b"\xff\xfa\xff\xff\xfe\xfb",
        )
        self.assertEqual(
            self.mask(b"\xff\xfb\xfd\xfc", b"\x00\x01\x02\x03\x04\x05"),
            b"\xff\xfa\xff\xff\xfb\xfe",
        )

    def test_length_validation(self: typing.Any):
        # Test all lengths of mask that are not 4 bytes.
        for mask in (b"", b"a", b"ab", b"abc", b"abcde", b"abcdef"):
            with self.subTest(mask=mask):
                with self.assertRaises(ValueError):
                    self.mask(mask, b"data asdf")


class PythonMaskFunctionTest(MaskFunctionMixin):
    def mask(self, mask, data):
        return _websocket_mask_python(mask, data)


@unittest.skipIf(speedups is None, "tornado.speedups module not present")
class CythonMaskFunctionTest(MaskFunctionMixin):
    def mask(self, mask, data):
        return speedups.websocket_mask(mask, data)


class ServerPeriodicPingTest(WebSocketBaseTestCase):
    def get_app(self):
        class PingHandler(TestWebSocketHandler):
            def on_pong(self, data):
                self.write_message("got pong")

        return Application(
            [("/", PingHandler)],
            websocket_ping_interval=0.01,
            websocket_ping_timeout=0,
        )

    @gen_test
    def test_server_ping(self):
        ws = yield self.ws_connect("/")
        for i in range(3):
            response = yield ws.read_message()
            self.assertEqual(response, "got pong")
        # TODO: test that the connection gets closed if ping responses stop.


class ClientPeriodicPingTest(WebSocketBaseTestCase):
    def get_app(self):
        class PingHandler(TestWebSocketHandler):
            def on_ping(self, data):
                self.write_message("got ping")

        return Application([("/", PingHandler)])

    @gen_test
    def test_client_ping(self):
        ws = yield self.ws_connect("/", ping_interval=0.01, ping_timeout=0)
        for i in range(3):
            response = yield ws.read_message()
            self.assertEqual(response, "got ping")
        ws.close()


class ServerPingTimeoutTest(WebSocketBaseTestCase):
    def get_app(self):
        self.handlers: list[WebSocketHandler] = []
        test = self

        class PingHandler(TestWebSocketHandler):
            def initialize(self, close_future=None, compression_options=None):
                self.handlers = test.handlers
                # capture the handler instance so we can interrogate it later
                self.handlers.append(self)
                return super().initialize(
                    close_future=close_future, compression_options=compression_options
                )

        app = Application([("/", PingHandler)])
        return app

    @staticmethod
    def install_hook(ws):
        """Optionally suppress the client's "pong" response."""

        ws.drop_pongs = False
        ws.pongs_received = 0

        def wrapper(fcn):
            def _inner(opcode: int, data: bytes):
                if opcode == 0xA:  # NOTE: 0x9=ping, 0xA=pong
                    ws.pongs_received += 1
                    if ws.drop_pongs:
                        # prevent pong responses
                        return
                # leave all other responses unchanged
                return fcn(opcode, data)

            return _inner

        ws.protocol._handle_message = wrapper(ws.protocol._handle_message)

    @gen_test
    def test_client_ping_timeout(self):
        # websocket client
        interval = 0.2
        ws = yield self.ws_connect(
            "/", ping_interval=interval, ping_timeout=interval / 4
        )
        self.install_hook(ws)

        # websocket handler (server side)
        handler = self.handlers[0]

        for _ in range(5):
            # wait for the ping period
            yield gen.sleep(interval)

            # connection should still be open from the server end
            self.assertIsNone(handler.close_code)
            self.assertIsNone(handler.close_reason)

            # connection should still be open from the client end
            assert ws.protocol.close_code is None

        # Check that our hook is intercepting messages; allow for
        # some variance in timing (due to e.g. cpu load)
        self.assertGreaterEqual(ws.pongs_received, 4)

        # suppress the pong response message
        ws.drop_pongs = True

        # give the server time to register this
        yield gen.sleep(interval * 1.5)

        # connection should be closed from the server side
        self.assertEqual(handler.close_code, 1000)
        self.assertEqual(handler.close_reason, "ping timed out")

        # client should have received a close operation
        self.assertEqual(ws.protocol.close_code, 1000)


class PingCalculationTest(unittest.TestCase):
    def test_ping_sleep_time(self):
        from tornado.websocket import WebSocketProtocol13

        now = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        interval = 10  # seconds
        last_ping_time = datetime.datetime(
            2025, 1, 1, 11, 59, 54, tzinfo=datetime.timezone.utc
        )
        sleep_time = WebSocketProtocol13.ping_sleep_time(
            last_ping_time=last_ping_time.timestamp(),
            interval=interval,
            now=now.timestamp(),
        )
        self.assertEqual(sleep_time, 4)


class ManualPingTest(WebSocketBaseTestCase):
    def get_app(self):
        class PingHandler(TestWebSocketHandler):
            def on_ping(self, data):
                self.write_message(data, binary=isinstance(data, bytes))

        return Application([("/", PingHandler)])

    @gen_test
    def test_manual_ping(self):
        ws = yield self.ws_connect("/")

        self.assertRaises(ValueError, ws.ping, "a" * 126)

        ws.ping("hello")
        resp = yield ws.read_message()
        # on_ping always sees bytes.
        self.assertEqual(resp, b"hello")

        ws.ping(b"binary hello")
        resp = yield ws.read_message()
        self.assertEqual(resp, b"binary hello")


class MaxMessageSizeTest(WebSocketBaseTestCase):
    def get_app(self):
        return Application([("/", EchoHandler)], websocket_max_message_size=1024)

    @gen_test
    def test_large_message(self):
        ws = yield self.ws_connect("/")

        # Write a message that is allowed.
        msg = "a" * 1024
        ws.write_message(msg)
        resp = yield ws.read_message()
        self.assertEqual(resp, msg)

        # Write a message that is too large.
        ws.write_message(msg + "b")
        resp = yield ws.read_message()
        # A message of None means the other side closed the connection.
        self.assertIs(resp, None)
        self.assertEqual(ws.close_code, 1009)
        self.assertEqual(ws.close_reason, "message too big")
        # TODO: Needs tests of messages split over multiple
        # continuation frames.
