"""HTTP access, the on-disk download cache, and the ATTACH options that steer both.

Three kinds of upstream are read:

* **Bulk files** (congress-legislators JSON, Voteview CSV, Senate roll-call XML)
  are downloaded on first use into a per-user cache directory and reused. A file
  older than the cache TTL is revalidated with ``If-None-Match`` /
  ``If-Modified-Since``; if the upstream is unreachable a stale copy is used and a
  warning is logged. Nothing is downloaded at import or ATTACH time, only when a
  query scans a relation that needs the file.
* **The Congress.gov API** is read live, page by page, with the API key sent in the
  ``X-Api-Key`` header (never in the URL, so it stays out of logs).

Every request has a timeout, and 429/5xx responses are retried with backoff that
honours ``Retry-After``.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from email.utils import formatdate
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

USER_AGENT = "vgi-us-congress/0.1 (+https://query.farm)"
TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 4
MAX_RETRY_SLEEP = 30.0
CHUNK = 1 << 20

DEFAULT_CONGRESS_GOV_URL = "https://api.congress.gov/v3"
DEFAULT_LEGISLATORS_URL = "https://unitedstates.github.io/congress-legislators"
DEFAULT_VOTEVIEW_URL = "https://voteview.com/static/data/out"
DEFAULT_SENATE_URL = "https://www.senate.gov/legislative/LIS"
DEFAULT_CACHE_TTL_HOURS = 24.0


class UpstreamError(RuntimeError):
    """An upstream returned an error we will not retry, or never answered."""


def default_cache_dir() -> Path:
    """Return the per-user cache directory (XDG on Linux, ~/Library/Caches on macOS, LOCALAPPDATA on Windows)."""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "vgi-us-congress"
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return Path(base) / "vgi-us-congress" / "Cache"
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "vgi-us-congress"


@dataclass(frozen=True, slots=True)
class Settings:
    """Where to read each upstream from and where to cache it, from the ATTACH options."""

    congress_gov_url: str = DEFAULT_CONGRESS_GOV_URL
    legislators_url: str = DEFAULT_LEGISLATORS_URL
    voteview_url: str = DEFAULT_VOTEVIEW_URL
    senate_url: str = DEFAULT_SENATE_URL
    cache_dir: Path | None = None
    cache_ttl_hours: float = DEFAULT_CACHE_TTL_HOURS

    @property
    def cache_root(self) -> Path:
        """The cache directory in use."""
        return self.cache_dir or default_cache_dir()


def settings_of(params: Any) -> Settings:
    """Read the ATTACH options of a bind/init/process call into `Settings`."""
    try:
        opts = dict(params.attach_options or {})
    except Exception:
        opts = {}

    def text(name: str, default: str) -> str:
        value = opts.get(name)
        return str(value).rstrip("/") if value else default

    cache_dir = opts.get("cache_dir")
    ttl = opts.get("cache_ttl_hours")
    return Settings(
        congress_gov_url=text("congress_gov_url", DEFAULT_CONGRESS_GOV_URL),
        legislators_url=text("legislators_url", DEFAULT_LEGISLATORS_URL),
        voteview_url=text("voteview_url", DEFAULT_VOTEVIEW_URL),
        senate_url=text("senate_url", DEFAULT_SENATE_URL),
        cache_dir=Path(str(cache_dir)).expanduser() if cache_dir else None,
        cache_ttl_hours=float(ttl) if ttl is not None else DEFAULT_CACHE_TTL_HOURS,
    )


def _retry_delay(err: urllib.error.HTTPError, attempt: int) -> float:
    retry_after = err.headers.get("Retry-After") if err.headers else None
    if retry_after and retry_after.strip().isdigit():
        delay: float = float(str(retry_after))
        return min(delay, MAX_RETRY_SLEEP)
    return min(0.5 * float(2**attempt), MAX_RETRY_SLEEP)


def _open(url: str, headers: dict[str, str]) -> Any:
    """Open ``url`` with retries on 429/5xx and network errors; return the response (caller closes it)."""
    merged = {"User-Agent": USER_AGENT, **headers}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(url, headers=merged)
        try:
            return urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS)
        except urllib.error.HTTPError as err:
            if err.code == 304:
                raise
            retryable = err.code == 429 or err.code >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                raise
            delay = _retry_delay(err, attempt)
            log.warning("GET %s returned HTTP %d; retrying in %.1fs", _redact(url), err.code, delay)
            err.close()
            time.sleep(delay)
        except urllib.error.URLError as err:
            if attempt == MAX_ATTEMPTS:
                raise UpstreamError(f"GET {_redact(url)} failed: {err.reason}") from err
            time.sleep(min(0.5 * 2**attempt, MAX_RETRY_SLEEP))
    raise AssertionError("unreachable")


def _redact(url: str) -> str:
    """Drop any api_key query parameter from a URL before it is logged or shown."""
    return re.sub(r"(?i)(api_key=)[^&]*", r"\1<redacted>", url)


def get_json(url: str, headers: dict[str, str] | None = None) -> Any:
    """GET ``url`` and decode JSON. HTTP errors raise `urllib.error.HTTPError` after retries."""
    with _open(url, {"Accept": "application/json", **(headers or {})}) as response:
        return json.load(response)


# ---------------------------------------------------------------- download cache


def cache_path(settings: Settings, url: str) -> Path:
    """Map an upstream URL to its file in the cache: ``<cache>/<host>/<path>``."""
    parts = urllib.parse.urlsplit(url)
    host = re.sub(r"[^A-Za-z0-9.-]", "_", parts.netloc) or "local"
    rel = [re.sub(r"[^A-Za-z0-9._-]", "_", p) for p in parts.path.split("/") if p and p not in {".", ".."}]
    return settings.cache_root.joinpath(host, *rel)


def _meta_path(path: Path) -> Path:
    return path.with_name(path.name + ".meta.json")


def read_meta(path: Path) -> dict[str, Any]:
    """Return the sidecar metadata recorded when ``path`` was downloaded (empty if none)."""
    try:
        data = json.loads(_meta_path(path).read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_meta(path: Path, meta: dict[str, Any]) -> None:
    tmp = _meta_path(path).with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(meta, indent=1, sort_keys=True))
    tmp.replace(_meta_path(path))


def cached_file(settings: Settings, url: str, *, ttl_hours: float | None = None) -> Path:
    """Return a local copy of ``url``, downloading or revalidating it as needed.

    Args:
        settings: ATTACH-derived settings (cache directory, TTL).
        url: The upstream file URL.
        ttl_hours: Override the TTL for this file (``None`` uses the ATTACH TTL).

    Returns:
        The path of the cached file.

    Raises:
        UpstreamError: The file is not cached and could not be downloaded.

    """
    path = cache_path(settings, url)
    ttl = settings.cache_ttl_hours if ttl_hours is None else ttl_hours
    meta = read_meta(path)
    if path.exists() and meta:
        age_hours = (time.time() - float(meta.get("fetched_at", 0))) / 3600.0
        if age_hours < ttl:
            return path
    try:
        _download(url, path, meta if path.exists() else {})
    except (UpstreamError, urllib.error.HTTPError, OSError) as err:
        if path.exists():
            log.warning("could not refresh %s (%s); using the cached copy", url, err)
            return path
        if isinstance(err, urllib.error.HTTPError):
            raise UpstreamError(f"GET {url} failed with HTTP {err.code}") from err
        raise UpstreamError(f"GET {url} failed: {err}") from err
    return path


def _download(url: str, path: Path, meta: dict[str, Any]) -> None:
    """Download ``url`` to ``path`` atomically, or confirm the cached copy with a conditional GET."""
    headers: dict[str, str] = {}
    if meta.get("etag"):
        headers["If-None-Match"] = str(meta["etag"])
    if meta.get("last_modified"):
        headers["If-Modified-Since"] = str(meta["last_modified"])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.part")
    try:
        response = _open(url, headers)
    except urllib.error.HTTPError as err:
        if err.code == 304:
            meta["fetched_at"] = time.time()
            meta["revalidated_at"] = formatdate(usegmt=True)
            _write_meta(path, meta)
            return
        raise
    digest = hashlib.sha256()
    size = 0
    with response, tmp.open("wb") as fh:
        expected = response.headers.get("Content-Length")
        while chunk := response.read(CHUNK):
            fh.write(chunk)
            digest.update(chunk)
            size += len(chunk)
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
    if expected is not None and expected.isdigit() and int(expected) != size:
        tmp.unlink(missing_ok=True)
        raise UpstreamError(f"GET {url}: truncated download ({size} of {expected} bytes)")
    tmp.replace(path)
    _write_meta(
        path,
        {
            "url": url,
            "etag": etag,
            "last_modified": last_modified,
            "fetched_at": time.time(),
            "bytes": size,
            "sha256": digest.hexdigest(),
        },
    )
    log.info("cached %s (%d bytes)", url, size)


def cache_entries(settings: Settings) -> list[dict[str, Any]]:
    """List every file in the cache with its recorded metadata."""
    root = settings.cache_root
    rows: list[dict[str, Any]] = []
    if not root.exists():
        return rows
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name.endswith((".meta.json", ".part", ".tmp")):
            continue
        meta = read_meta(path)
        with contextlib.suppress(OSError):
            rows.append(
                {
                    "path": str(path),
                    "url": meta.get("url"),
                    "bytes": path.stat().st_size,
                    "fetched_at": meta.get("fetched_at"),
                    "etag": meta.get("etag"),
                    "last_modified": meta.get("last_modified"),
                    "sha256": meta.get("sha256"),
                }
            )
    return rows
