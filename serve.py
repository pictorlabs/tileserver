"""High-performance OpenSlide tile server.

Pre-fork multi-process architecture: N worker processes each with their own
OpenSlide handles and tile caches. Eliminates Python GIL contention for
true parallel tile rendering.

Endpoints:
    GET /                          → health check (no auth)
    GET /slides                    → list source slides
    GET /stains                    → list stain output directories
    GET /stains/{job_id}           → list files in a stain output
    GET /all                       → list all slides (source + stain)
    GET /slides/{slide_id}.dzi     → DeepZoom descriptor (XML)
    GET /slides/{slide_id}/{level}/{col}_{row}.jpeg  → tile
    GET /slides/{slide_id}/info    → slide metadata
    GET /slides/{slide_id}/thumbnail?max_size=512  → thumbnail
    GET /slides/{slide_id}/download → full file download
    GET /slides/{slide_id}/region   → region extract

Environment:
    SLIDE_DIR      → source scans directory (default: /data)
    STAIN_DIR      → stain outputs directory (default: /stains)
    SERVE_PORT     → listen port (default: 8080)
    TILE_SIZE      → tile size in pixels (default: 254)
    OVERLAP        → tile overlap in pixels (default: 1)
    WORKERS        → number of worker processes (default: CPU count)
    AUTH0_DOMAIN   → Auth0 tenant domain. If unset, auth disabled.
    AUTH0_AUDIENCE → expected JWT audience
"""

import io
import json
import os
import re
import signal
import socket
import sys
import time
import threading
from collections import OrderedDict
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen

import openslide
from openslide.deepzoom import DeepZoomGenerator

SLIDE_DIR = Path(os.environ.get("SLIDE_DIR", "/data"))
STAIN_DIR = Path(os.environ.get("STAIN_DIR", "/stains"))
PORT = int(os.environ.get("SERVE_PORT", os.environ.get("PORT", "8080")))
TILE_SIZE = int(os.environ.get("TILE_SIZE", "254"))
OVERLAP = int(os.environ.get("OVERLAP", "1"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "80"))
NUM_WORKERS = int(os.environ.get("WORKERS", os.cpu_count() or 4))

AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN", "")
AUTH0_AUDIENCE = os.environ.get("AUTH0_AUDIENCE", "")

# ── JWT validation ──
_jwks_cache: dict | None = None
_jwks_cache_time: float = 0
JWKS_CACHE_TTL = 3600


def _get_jwks() -> dict:
    global _jwks_cache, _jwks_cache_time
    if _jwks_cache and time.time() - _jwks_cache_time < JWKS_CACHE_TTL:
        return _jwks_cache
    url = f"{AUTH0_DOMAIN.rstrip('/')}/.well-known/jwks.json"
    with urlopen(Request(url), timeout=10) as resp:
        _jwks_cache = json.loads(resp.read())
        _jwks_cache_time = time.time()
        return _jwks_cache


def _verify_jwt(token: str) -> dict | None:
    try:
        import jwt as pyjwt
        jwks = _get_jwks()
        header = pyjwt.get_unverified_header(token)
        for key in jwks.get("keys", []):
            if key.get("kid") == header.get("kid"):
                public_key = pyjwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key))
                return pyjwt.decode(
                    token, public_key, algorithms=["RS256"],
                    audience=AUTH0_AUDIENCE,
                    issuer=f"{AUTH0_DOMAIN.rstrip('/')}/",
                )
        return None
    except Exception:
        return None


def _check_auth(headers, query_params=None) -> tuple[bool, str]:
    if not AUTH0_DOMAIN:
        return True, ""
    auth = headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        payload = _verify_jwt(auth.split(" ", 1)[1])
        if payload is not None:
            return True, ""
    if query_params:
        token_list = query_params.get("token", [])
        if token_list:
            payload = _verify_jwt(token_list[0])
            if payload is not None:
                return True, ""
    if not auth and not (query_params and query_params.get("token")):
        return False, "Missing or invalid Authorization header"
    return False, "Invalid or expired token"


# ── Slide index (shared read-only after rebuild) ──
SUPPORTED_EXT = {".svs", ".tiff", ".tif", ".ndpi", ".mrxs", ".scn", ".bif", ".vms"}

_slide_index: dict[str, Path] = {}
_stain_index: dict[str, Path] = {}
_index_time: float = 0
INDEX_TTL = 120


def _rebuild_index():
    global _slide_index, _stain_index, _index_time
    if time.time() - _index_time < INDEX_TTL:
        return

    slides = {}
    if SLIDE_DIR.exists():
        for f in SLIDE_DIR.iterdir():
            if f.suffix.lower() in SUPPORTED_EXT and f.is_file():
                slides[f.stem] = f

    stains = {}
    if STAIN_DIR.exists():
        for job_dir in STAIN_DIR.iterdir():
            if not job_dir.is_dir():
                continue
            for f in job_dir.iterdir():
                if f.suffix.lower() in SUPPORTED_EXT and f.is_file():
                    if ".ome." in f.name.lower():
                        continue
                    stain_id = f"stain__{job_dir.name}__{f.stem}"
                    stains[stain_id] = f

    _slide_index = slides
    _stain_index = stains
    _index_time = time.time()


def _resolve_slide(slide_id: str) -> Path:
    _rebuild_index()
    if slide_id in _slide_index:
        return _slide_index[slide_id]
    if slide_id in _stain_index:
        return _stain_index[slide_id]
    for sid, path in _stain_index.items():
        if slide_id in sid:
            return path
    raise KeyError(f"Slide not found: {slide_id}")


# ── Per-worker slide + tile cache ──
_slide_cache: dict[str, tuple[openslide.OpenSlide, DeepZoomGenerator]] = {}
_slide_cache_lock = threading.Lock()

# LRU tile cache — OrderedDict for fast eviction
_tile_cache: OrderedDict[tuple, bytes] = OrderedDict()
_tile_cache_lock = threading.Lock()
TILE_CACHE_MAX = 20000  # ~1GB at ~50KB avg


def _get_slide(slide_id: str) -> tuple[openslide.OpenSlide, DeepZoomGenerator]:
    with _slide_cache_lock:
        if slide_id in _slide_cache:
            return _slide_cache[slide_id]
    path = _resolve_slide(slide_id)
    osr = openslide.OpenSlide(str(path))
    dz = DeepZoomGenerator(osr, tile_size=TILE_SIZE, overlap=OVERLAP)
    with _slide_cache_lock:
        _slide_cache[slide_id] = (osr, dz)
    return osr, dz


def _get_tile(slide_id: str, level: int, col: int, row: int) -> bytes:
    key = (slide_id, level, col, row)
    with _tile_cache_lock:
        if key in _tile_cache:
            _tile_cache.move_to_end(key)  # LRU touch
            return _tile_cache[key]

    _, dz = _get_slide(slide_id)
    tile = dz.get_tile(level, (col, row))
    buf = io.BytesIO()
    tile.save(buf, format="JPEG", quality=JPEG_QUALITY)
    data = buf.getvalue()

    with _tile_cache_lock:
        _tile_cache[key] = data
        while len(_tile_cache) > TILE_CACHE_MAX:
            _tile_cache.popitem(last=False)  # evict oldest
    return data


# ── DZI cache (avoid re-generating XML string) ──
_dzi_cache: dict[str, str] = {}


def _get_dzi(slide_id: str) -> str:
    if slide_id in _dzi_cache:
        return _dzi_cache[slide_id]
    _, dz = _get_slide(slide_id)
    xml = dz.get_dzi("jpeg")
    _dzi_cache[slide_id] = xml
    return xml


# ── Pre-warming ──
def _prewarm_stains():
    _rebuild_index()
    for sid in list(_stain_index.keys()):
        try:
            _get_slide(sid)
            _get_dzi(sid)  # cache DZI too
            print(f"  [worker {os.getpid()}] Pre-warmed: {sid}", flush=True)
        except Exception as e:
            print(f"  [worker {os.getpid()}] Warm failed {sid}: {e}", flush=True)


# ── HTTP Handler ──
class TileHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive by default

    def log_message(self, fmt, *args):
        pass

    def _send(self, data: bytes, content_type: str, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=86400")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status: int, msg: str):
        body = json.dumps({"error": msg}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        qs = parse_qs(parsed.query)

        if path == "" or path == "/":
            self._send(b'{"status":"ok"}', "application/json")
            return

        ok, err = _check_auth(self.headers, qs)
        if not ok:
            self._error(401, err)
            return

        # ── List endpoints ──
        if path == "/slides":
            _rebuild_index()
            items = [{"id": sid, "filename": fp.name, "size_bytes": fp.stat().st_size, "type": "source"}
                     for sid, fp in sorted(_slide_index.items())]
            self._send(json.dumps(items).encode(), "application/json")
            return

        if path == "/stains":
            _rebuild_index()
            jobs: dict[str, list] = {}
            for sid, fp in sorted(_stain_index.items()):
                parts = sid.split("__")
                jid = parts[1] if len(parts) >= 2 else "unknown"
                jobs.setdefault(jid, []).append({"slide_id": sid, "filename": fp.name, "size_bytes": fp.stat().st_size})
            self._send(json.dumps([{"job_id": j, "files": f} for j, f in sorted(jobs.items())]).encode(), "application/json")
            return

        m = re.match(r"^/stains/([^/]+)$", path)
        if m:
            job_id = m.group(1)
            _rebuild_index()
            files = [{"slide_id": sid, "filename": fp.name, "size_bytes": fp.stat().st_size}
                     for sid, fp in _stain_index.items() if f"__{job_id}__" in sid]
            if not files:
                self._error(404, f"No stain outputs for job: {job_id}")
                return
            self._send(json.dumps(files).encode(), "application/json")
            return

        if path == "/all":
            _rebuild_index()
            items = [{"id": sid, "filename": fp.name, "size_bytes": fp.stat().st_size, "type": "source"}
                     for sid, fp in sorted(_slide_index.items())]
            items += [{"id": sid, "filename": fp.name, "size_bytes": fp.stat().st_size, "type": "stain"}
                      for sid, fp in sorted(_stain_index.items())]
            self._send(json.dumps(items).encode(), "application/json")
            return

        # ── DZI ──
        m = re.match(r"^/slides/([^/]+)\.dzi$", path)
        if m:
            slide_id = m.group(1)
            try:
                xml = _get_dzi(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            self._send(xml.encode(), "application/xml")
            return

        # ── Tile ──
        m = re.match(r"^/slides/([^/]+)/(\d+)/(\d+)_(\d+)\.jpeg$", path)
        if m:
            slide_id = m.group(1)
            level, col, row = int(m.group(2)), int(m.group(3)), int(m.group(4))
            try:
                data = _get_tile(slide_id, level, col, row)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            except (ValueError, openslide.OpenSlideError) as e:
                self._error(400, str(e))
                return
            self._send(data, "image/jpeg")
            return

        # ── Info ──
        m = re.match(r"^/slides/([^/]+)/info$", path)
        if m:
            slide_id = m.group(1)
            try:
                osr, dz = _get_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            _rebuild_index()
            info = {
                "id": slide_id,
                "type": "source" if slide_id in _slide_index else "stain",
                "dimensions": osr.dimensions,
                "level_count": osr.level_count,
                "level_dimensions": list(osr.level_dimensions),
                "level_downsamples": list(osr.level_downsamples),
                "mpp_x": osr.properties.get(openslide.PROPERTY_NAME_MPP_X),
                "mpp_y": osr.properties.get(openslide.PROPERTY_NAME_MPP_Y),
                "objective_power": osr.properties.get(openslide.PROPERTY_NAME_OBJECTIVE_POWER),
                "vendor": osr.properties.get(openslide.PROPERTY_NAME_VENDOR),
                "deepzoom_levels": dz.level_count,
                "deepzoom_tile_count": dz.tile_count,
                "tile_size": TILE_SIZE,
                "overlap": OVERLAP,
            }
            self._send(json.dumps(info).encode(), "application/json")
            return

        # ── Thumbnail ──
        m = re.match(r"^/slides/([^/]+)/thumbnail$", path)
        if m:
            slide_id = m.group(1)
            max_size = int(qs.get("max_size", [512])[0])
            try:
                osr, _ = _get_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            thumb = osr.get_thumbnail((max_size, max_size))
            buf = io.BytesIO()
            thumb.save(buf, format="JPEG", quality=JPEG_QUALITY)
            self._send(buf.getvalue(), "image/jpeg")
            return

        # ── Download ──
        m = re.match(r"^/slides/([^/]+)/download$", path)
        if m:
            slide_id = m.group(1)
            try:
                fpath = _resolve_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            file_size = fpath.stat().st_size
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(file_size))
            self.send_header("Content-Disposition", f'attachment; filename="{fpath.name}"')
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with open(fpath, "rb") as f:
                while chunk := f.read(4 * 1024 * 1024):  # 4MB chunks
                    self.wfile.write(chunk)
            return

        # ── Region ──
        m = re.match(r"^/slides/([^/]+)/region$", path)
        if m:
            slide_id = m.group(1)
            try:
                osr, _ = _get_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            try:
                x, y = int(qs.get("x", [0])[0]), int(qs.get("y", [0])[0])
                w, h = int(qs.get("w", [1024])[0]), int(qs.get("h", [1024])[0])
                level = int(qs.get("level", [0])[0])
                fmt = qs.get("format", ["jpeg"])[0].lower()
            except (ValueError, IndexError):
                self._error(400, "Invalid region parameters")
                return
            w, h = min(w, 8192), min(h, 8192)
            level = min(level, osr.level_count - 1)
            try:
                region = osr.read_region((x, y), level, (w, h)).convert("RGB")
            except Exception as e:
                self._error(400, f"Region read failed: {e}")
                return
            buf = io.BytesIO()
            if fmt == "png":
                region.save(buf, format="PNG")
                self._send(buf.getvalue(), "image/png")
            else:
                region.save(buf, format="JPEG", quality=JPEG_QUALITY)
                self._send(buf.getvalue(), "image/jpeg")
            return

        self._error(404, "Not found")

    def do_HEAD(self):
        """Handle HEAD — same as GET but no body (needed for download preflight)."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        qs = parse_qs(parsed.query)

        if path == "" or path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "15")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            return

        ok, err = _check_auth(self.headers, qs)
        if not ok:
            body = json.dumps({"error": err}).encode()
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            return

        # Download HEAD — return file size without body
        m = re.match(r"^/slides/([^/]+)/download$", path)
        if m:
            slide_id = m.group(1)
            try:
                fpath = _resolve_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            file_size = fpath.stat().st_size
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(file_size))
            self.send_header("Content-Disposition", f'attachment; filename="{fpath.name}"')
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            return

        self._error(404, "Not found")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Connection", "keep-alive")
        self.end_headers()


class WorkerHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    request_queue_size = 128


def _run_worker(sock: socket.socket, worker_id: int):
    """Run one worker process attached to the shared listening socket."""
    print(f"  Worker {worker_id} (pid {os.getpid()}) starting", flush=True)

    server = WorkerHTTPServer(("0.0.0.0", PORT), TileHandler)
    server.socket = sock  # reuse shared socket
    server.server_address = sock.getsockname()

    # Pre-warm stain slides in background thread (don't block health checks)
    threading.Thread(target=_prewarm_stains, daemon=True).start()

    print(f"  Worker {worker_id} (pid {os.getpid()}) ready", flush=True)
    server.serve_forever()


def main():
    print(f"Tileserver starting on :{PORT}")
    print(f"  SLIDE_DIR:    {SLIDE_DIR}")
    print(f"  STAIN_DIR:    {STAIN_DIR}")
    print(f"  TILE_SIZE:    {TILE_SIZE}")
    print(f"  OVERLAP:      {OVERLAP}")
    print(f"  JPEG_QUALITY: {JPEG_QUALITY}")
    print(f"  WORKERS:      {NUM_WORKERS}")

    _rebuild_index()
    print(f"  Source slides: {len(_slide_index)}")
    print(f"  Stain outputs: {len(_stain_index)}")

    # Create shared listening socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass
    sock.bind(("0.0.0.0", PORT))
    sock.listen(256)
    print(f"Listening on :{PORT}", flush=True)

    # Serve health checks immediately from parent while workers start
    # (Knative readiness probe hits / within seconds)
    # Fork worker processes
    workers: list[int] = []
    for i in range(NUM_WORKERS):
        pid = os.fork()
        if pid == 0:
            # Child worker
            signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
            _run_worker(sock, i)
            sys.exit(0)
        workers.append(pid)

    print(f"Spawned {NUM_WORKERS} workers: {workers}", flush=True)

    # Parent: handle SIGTERM gracefully
    def _shutdown(*_):
        print("Shutting down workers...", flush=True)
        for pid in workers:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for pid in workers:
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Wait for any child to exit (shouldn't happen)
    while True:
        try:
            pid, status = os.wait()
            print(f"Worker {pid} exited with status {status}, restarting...", flush=True)
            # Respawn
            new_pid = os.fork()
            if new_pid == 0:
                signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
                _run_worker(sock, -1)
                sys.exit(0)
            workers = [p for p in workers if p != pid] + [new_pid]
        except ChildProcessError:
            break


if __name__ == "__main__":
    main()
