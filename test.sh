#!/bin/sh
# Manual test runner for the WebSocket changes in tornado/websocket.py:
#
#   1. RFC 7692 permessage-deflate negotiation
#      (server_no_context_takeover / client_no_context_takeover /
#       server_max_window_bits / client_max_window_bits)
#   2. Per-client-IP connection rate limiting (sliding window, 429 +
#      immediate IOStream close)
#   3. Immediate release of zlib compression contexts on the close path
#
# Usage:
#   ./test.sh                 # run the whole websocket test suite
#   ./test.sh -v              # verbose output
#   ./test.sh tornado.test.websocket_test.WebSocketRateLimiterTest
#                             # run a single test class (or method)
#
# Pure unit tests (no sockets required):
#   ./test.sh tornado.test.websocket_test.PerMessageDeflateExtensionParsingTest \
#             tornado.test.websocket_test.PerMessageDeflateNegotiationTest \
#             tornado.test.websocket_test.WebSocketRateLimiterTest
#
# Networked integration tests (require permission to bind/connect on
# 127.0.0.1):
#   ./test.sh tornado.test.websocket_test.PerMessageDeflateHandshakeTest \
#             tornado.test.websocket_test.WebSocketRateLimitTest \
#             tornado.test.websocket_test.CompressionContextReleaseTest

set -e
cd "$(dirname "$0")"

if [ "$#" -eq 0 ]; then
    set -- tornado.test.websocket_test
fi

exec python3 -m tornado.test.runtests "$@"
