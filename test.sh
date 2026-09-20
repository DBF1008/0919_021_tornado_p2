#!/bin/sh
# Run the unit tests covering the WebSocket changes:
#   - permessage-deflate (RFC 7692) extension negotiation
#   - per-IP handshake rate limiting (429 + stream close)
#   - immediate release of compression contexts on close
# plus the httputil header-parsing tests they rely on.
#
# Usage:
#   ./test.sh                                            # affected test modules
#   ./test.sh tornado.test.websocket_test.WebSocketRateLimitTest  # one test class
#   ./test.sh --verbose                                  # unittest verbosity
#
# For the complete Tornado test suite, use ./runtests.sh instead.

cd $(dirname $0)

if [ $# -eq 0 ]; then
    set -- tornado.test.websocket_test tornado.test.httputil_test
fi

# "python -m" differs from "python tornado/test/runtests.py" in how it sets
# up the default python path.  "python -m" uses the current directory,
# while "python file.py" uses the directory containing "file.py".
exec python -m tornado.test.runtests "$@"
