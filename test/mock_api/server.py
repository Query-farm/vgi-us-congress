#!/usr/bin/env python3
"""Offline stand-in for every upstream the worker reads, serving ``test/fixtures``.

Endpoints (mount points match the worker's ATTACH URL options):

``/legislators/<file>``  congress-legislators JSON (``legislators_url``)
``/voteview/<kind>/<file>``  Voteview CSVs (``voteview_url``)
``/senate/<path>``  Senate LIS roll-call XML (``senate_url``)
``/congress/v3/<path>``  Congress.gov API v3 (``congress_gov_url``)

The Congress.gov mock requires ``X-Api-Key: test-key`` (else 403, as api.data.gov
answers), serves ``fixtures/congress/<path>.json``, and re-paginates every list
with at most ``MOCK_PAGE_CAP`` (default 2) items per page, filling
``pagination.next`` the way the real API does, so the worker's paging loop is
exercised. ``fromDateTime`` / ``toDateTime`` filter the list on ``updateDate``.
With ``MOCK_API_FLAKY=1`` the first request for each URL gets ``429`` with
``Retry-After: 0``. Static files send ``ETag`` and honour ``If-None-Match``.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
API_KEY = "test-key"
PAGE_CAP = int(os.environ.get("MOCK_PAGE_CAP", "2"))
FLAKY = os.environ.get("MOCK_API_FLAKY") == "1"

_seen: set[str] = set()
_lock = threading.Lock()


def _ts(value: Any) -> dt.datetime | None:
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def _list_slot(body: dict[str, Any]) -> tuple[dict[str, Any], str] | None:
    """Find the paged list in a response: a top-level list, or a nested one."""
    for key, value in body.items():
        if isinstance(value, list):
            return body, key
    for value in body.values():
        if isinstance(value, dict):
            for key in ("legislativeSubjects", "results"):
                if isinstance(value.get(key), list):
                    return value, key
    return None


class Handler(BaseHTTPRequestHandler):
    """Serve fixtures."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("mock_api: " + (format % args) + "\n")

    def _send(self, status: int, payload: bytes, ctype: str, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: int, body: object) -> None:
        self._send(status, json.dumps(body).encode(), "application/json")

    def do_GET(self) -> None:
        """Route a GET."""
        url = urlparse(self.path)
        if FLAKY:
            with _lock:
                first = self.path not in _seen
                _seen.add(self.path)
            if first:
                self._send(429, b'{"error": "slow down"}', "application/json", {"Retry-After": "0"})
                return
        if url.path == "/health":
            self._json(200, {"status": "ok"})
        elif url.path.startswith("/congress/v3/"):
            self._congress(url.path.removeprefix("/congress/v3/"), parse_qs(url.query))
        elif url.path.split("/")[1] in {"legislators", "voteview", "senate"}:
            self._static(url.path.lstrip("/"))
        else:
            self._json(404, {"error": "not found"})

    def _static(self, rel: str) -> None:
        path = (FIXTURES / rel).resolve()
        if FIXTURES not in path.parents or not path.is_file():
            self._send(404, b"<html>Not Found</html>", "text/html")
            return
        data = path.read_bytes()
        etag = '"' + hashlib.sha256(data).hexdigest()[:16] + '"'
        if self.headers.get("If-None-Match") == etag:
            self.send_response(304)
            self.send_header("ETag", etag)
            self.end_headers()
            return
        ctype = {".json": "application/json", ".csv": "text/csv", ".xml": "application/xml"}.get(
            path.suffix, "text/plain"
        )
        self._send(200, data, ctype, {"ETag": etag, "Last-Modified": "Sun, 15 Nov 2026 06:00:00 GMT"})

    def _congress(self, rel: str, query: dict[str, list[str]]) -> None:
        if self.headers.get("X-Api-Key") != API_KEY:
            self._json(403, {"error": {"code": "API_KEY_INVALID", "message": "An invalid api_key was supplied."}})
            return
        path = (FIXTURES / "congress" / f"{rel.rstrip('/')}.json").resolve()
        if FIXTURES not in path.parents or not path.is_file():
            self._json(404, {"error": f"Unknown resource: {rel}"})
            return
        body: dict[str, Any] = json.loads(path.read_text())
        slot = _list_slot(body)
        if slot is None:
            self._json(200, body)
            return
        holder, key = slot
        items: list[Any] = holder[key]
        lo, hi = _ts((query.get("fromDateTime") or [None])[0]), _ts((query.get("toDateTime") or [None])[0])
        if lo or hi:
            items = [
                i
                for i in items
                if (u := _ts(i.get("updateDate"))) is None or ((not lo or u >= lo) and (not hi or u <= hi))
            ]
        offset = int((query.get("offset") or ["0"])[0])
        limit = min(int((query.get("limit") or ["20"])[0]), PAGE_CAP)
        holder[key] = items[offset : offset + limit]
        body["pagination"] = {"count": len(items)}
        if offset + limit < len(items):
            nxt = {k: v[0] for k, v in query.items() if k != "offset"}
            nxt["offset"] = str(offset + limit)
            body["pagination"]["next"] = f"http://{self.headers.get('Host')}/congress/v3/{rel}?{urlencode(nxt)}"
        self._json(200, body)


def main() -> None:
    """Serve on 127.0.0.1 and print ``PORT:<n>``."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
