"""Receive a TCE-MG proxy token from a browser tab and write it to disk.

WHY THIS EXISTS
---------------
`harvest_mg.py` reads its token from a file and parks when that token expires.
The token lives 120 minutes, a full harvest runs ~20 hours, and the only issuer
is a portal page load scored by reCAPTCHA v3 -- there is no refresh endpoint and
no service credential (see TCE_MG_CREDENTIAL_REQUEST.md). So the file has to be
rewritten roughly ten times per harvest, and the value is a 5 KB JWT that nobody
should be copy-pasting through a terminal that often.

This is a one-endpoint HTTP server bound to loopback. A tab already open on the
portal POSTs `localStorage.tokenAuthorizationProxy` to it; it validates the shape,
checks there is time left, and writes the file the harvester is watching. The
harvester notices the new mtime within a minute and resumes on its own.

It does NOT mint tokens, drive a browser, or touch reCAPTCHA. A human (or an
already-open tab they control) still loads the page; this only moves the result
the last few inches onto disk.

SCOPE AND SAFETY
----------------
Bound to 127.0.0.1 and refuses to serve anything else. It accepts a token from
any local origin, which is the point -- the portal page is on a different origin
and needs CORS to reach it -- so do not run it on a shared machine while
untrusted local code is running. It writes exactly one file, mode 0600, and
prints nothing sensitive: the log line carries the expiry, never the token.

Usage:
    python mg_token_bridge.py --token-file ~/mg_token.txt &

then, in the console of a tab open on https://dadosabertos.tce.mg.gov.br/ :

    fetch('http://127.0.0.1:8787/token', {method:'POST',
          body: localStorage.getItem('tokenAuthorizationProxy')})

`--bookmarklet` prints a one-click version of that. It sends whatever token the
tab currently holds and does NOT reload first -- a reload tears down the page
before the POST could run, so the two steps cannot live in one bookmarklet. When
the tab's token is nearly expired, reload the tab yourself and click again; the
bridge refuses an expired token loudly rather than writing it.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

DEFAULT_PORT = 8787


def minutes_left(token: str) -> float | None:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload))["exp"]
    except Exception:
        return None
    return (exp - datetime.now(tz=UTC).timestamp()) / 60


# The only page that has any business posting here. Binding to loopback is NOT
# a second lock: any page open in any browser on this machine can POST to
# 127.0.0.1, and a `text/plain` body makes it a CORS "simple request" that is
# sent without a preflight at all. `*` would therefore let a hostile tab
# overwrite the token file with a JWT it minted itself -- the shape and `exp`
# checks below both pass on a self-signed one -- after which the harvester gets
# 401 from the portal and parks until a human looks at it.
ALLOWED_ORIGIN = "https://dadosabertos.tce.mg.gov.br"


class Handler(BaseHTTPRequestHandler):
    token_path: Path = Path("mg_token.txt")

    def _origin_ok(self) -> bool:
        return self.headers.get("Origin") == ALLOWED_ORIGIN

    def _cors(self):
        # The portal page is a different origin, so the POST is cross-origin and
        # needs these.
        self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        # Chrome's Private Network Access: a public https origin reaching a
        # loopback address is blocked outright unless the preflight is answered
        # with this header. Without it the page sees a bare "TypeError: Failed to
        # fetch" with no CORS message, which reads like the server is not running.
        self.send_header("Access-Control-Allow-Private-Network", "true")

    def _reply(self, code: int, body: str):
        payload = body.encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_OPTIONS(self):
        if not self._origin_ok():
            self._reply(403, "forbidden origin")
            return
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_POST(self):
        if not self._origin_ok():
            self._reply(403, "forbidden origin")
            return
        if self.path.rstrip("/") not in ("/token", ""):
            self._reply(404, "not found")
            return
        length = int(self.headers.get("Content-Length") or 0)
        token = self.rfile.read(length).decode("utf-8", "replace").strip()
        if token.lower().startswith("token "):
            token = token[len("token ") :].strip()
        # Shape first: writing a truncated or empty value would put the harvester
        # into a token-expired loop that looks like a portal outage.
        if token.count(".") != 2 or len(token) < 500:
            self._reply(400, f"not a TCE-MG JWT (len {len(token)})")
            return
        left = minutes_left(token)
        if left is None:
            self._reply(400, "token payload unreadable")
            return
        if left <= 1:
            self._reply(400, f"token already expired ({left:.0f} min)")
            return
        # Written via a temp file in the same directory and renamed, so the
        # harvester -- which may read at any instant from eight threads -- never
        # sees a half-written token.
        temporary = self.token_path.with_suffix(".tmp")
        temporary.write_text(token)
        os.chmod(temporary, 0o600)
        temporary.replace(self.token_path)
        stamp = datetime.now(tz=UTC).isoformat(timespec="seconds")
        print(f"[{stamp}] token accepted, {left:.0f} min left", flush=True)
        self._reply(200, f"ok, {left:.0f} min")

    def log_message(self, format, *args):
        pass


# Deliberately does not reload: `location.reload()` would tear down the page
# before the POST could run, so a self-reloading bookmarklet silently sends
# nothing. Reload the tab first, then click this.
BOOKMARKLET = """
javascript:(async()=>{const p=%d;
const t=localStorage.getItem('tokenAuthorizationProxy');
if(!t){alert('no token in this tab -- reload the portal page first');return;}
try{const r=await fetch('http://127.0.0.1:'+p+'/token',{method:'POST',body:t});
alert('token bridge: '+await r.text());}
catch(e){alert('bridge unreachable on port '+p+': '+e.message);}})()
""".strip().replace("\n", "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--token-file", default=os.environ.get("MG_TOKEN_FILE")
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--bookmarklet",
        action="store_true",
        help="print a one-click refresher",
    )
    args = parser.parse_args()
    if args.bookmarklet:
        print(BOOKMARKLET % args.port)
        return
    if not args.token_file:
        raise SystemExit("--token-file or $MG_TOKEN_FILE required")
    Handler.token_path = Path(args.token_file).expanduser()
    server = HTTPServer(("127.0.0.1", args.port), Handler)
    print(
        f"token bridge on http://127.0.0.1:{args.port}/token -> "
        f"{Handler.token_path}\nfrom a tab on the portal:\n"
        f"  fetch('http://127.0.0.1:{args.port}/token',{{method:'POST',"
        f"body:localStorage.getItem('tokenAuthorizationProxy')}})",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
