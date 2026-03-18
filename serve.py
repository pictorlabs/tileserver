"""Minimal OpenSlide tile server for whole-slide images.

Serves DeepZoom-compatible tiles for integration with OpenSeadragon.
Supports multiple slide directories (source scans + stain outputs).

Endpoints:
    GET /                          → health check (no auth)
    GET /slides                    → list source slides
    GET /stains                    → list stain output directories
    GET /stains/{job_id}           → list files in a stain output
    GET /slides/{slide_id}.dzi     → DeepZoom descriptor (XML)
    GET /slides/{slide_id}/{level}/{col}_{row}.jpeg  → tile
    GET /slides/{slide_id}/info    → slide metadata
    GET /slides/{slide_id}/thumbnail?max_size=512  → thumbnail

Slide ID resolution:
    1. Check SLIDE_DIR (source scans: /data/dash/scans)
    2. Check STAIN_DIR subdirectories (stain outputs: /artifacts/stains/{job_id}/*.tiff)
    For stain outputs, use "{job_id}__{filename_stem}" as the slide_id,
    or just the filename stem if unique.

Environment:
    SLIDE_DIR      → source scans directory (default: /data)
    STAIN_DIR      → stain outputs directory (default: /stains)
    SERVE_PORT     → listen port (default: 8080)
    TILE_SIZE      → tile size in pixels (default: 254)
    OVERLAP        → tile overlap in pixels (default: 1)
    AUTH0_DOMAIN   → Auth0 tenant domain. If unset, auth disabled.
    AUTH0_AUDIENCE → expected JWT audience
"""

import io
import json
import os
import re
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen

import openslide
from openslide.deepzoom import DeepZoomGenerator
from PIL import Image

SLIDE_DIR = Path(os.environ.get("SLIDE_DIR", "/data"))
STAIN_DIR = Path(os.environ.get("STAIN_DIR", "/stains"))
PORT = int(os.environ.get("SERVE_PORT", os.environ.get("PORT", "8080")))
TILE_SIZE = int(os.environ.get("TILE_SIZE", "254"))
OVERLAP = int(os.environ.get("OVERLAP", "1"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "80"))

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

def _check_auth(headers) -> tuple[bool, str]:
    if not AUTH0_DOMAIN:
        return True, ""
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False, "Missing or invalid Authorization header"
    payload = _verify_jwt(auth.split(" ", 1)[1])
    if payload is None:
        return False, "Invalid or expired token"
    return True, ""

# ── Slide resolution ──
SUPPORTED_EXT = {".svs", ".tiff", ".tif", ".ndpi", ".mrxs", ".scn", ".bif", ".vms"}

# Cache: slide_id → (OpenSlide, DeepZoomGenerator)
_cache: dict[str, tuple[openslide.OpenSlide, DeepZoomGenerator]] = {}
_cache_lock = threading.Lock()

# Slide index: slide_id → Path (rebuilt periodically)
_slide_index: dict[str, Path] = {}
_stain_index: dict[str, Path] = {}  # job_id__stem → path
_index_time: float = 0
INDEX_TTL = 60  # rebuild every 60s


def _rebuild_index():
    """Rebuild the slide index from SLIDE_DIR and STAIN_DIR."""
    global _slide_index, _stain_index, _index_time
    if time.time() - _index_time < INDEX_TTL:
        return

    slides = {}
    # Source scans
    if SLIDE_DIR.exists():
        for f in SLIDE_DIR.iterdir():
            if f.suffix.lower() in SUPPORTED_EXT and f.is_file():
                slides[f.stem] = f

    stains = {}
    # Stain outputs: /stains/{job_id}/*.tiff
    if STAIN_DIR.exists():
        for job_dir in STAIN_DIR.iterdir():
            if not job_dir.is_dir():
                continue
            for f in job_dir.iterdir():
                if f.suffix.lower() in SUPPORTED_EXT and f.is_file():
                    # Skip .ome.tiff (single level) — prefer the pyramidal .tiff
                    if ".ome." in f.name.lower():
                        continue
                    stain_id = f"stain__{job_dir.name}__{f.stem}"
                    stains[stain_id] = f

    _slide_index = slides
    _stain_index = stains
    _index_time = time.time()


def _resolve_slide(slide_id: str) -> Path:
    """Resolve a slide_id to a file path. Checks source scans then stain outputs."""
    _rebuild_index()
    if slide_id in _slide_index:
        return _slide_index[slide_id]
    if slide_id in _stain_index:
        return _stain_index[slide_id]
    # Fuzzy: check if it's a job_id prefix match in stains
    for sid, path in _stain_index.items():
        if slide_id in sid:
            return path
    raise KeyError(f"Slide not found: {slide_id}")


def _get_slide(slide_id: str) -> tuple[openslide.OpenSlide, DeepZoomGenerator]:
    with _cache_lock:
        if slide_id in _cache:
            return _cache[slide_id]
    path = _resolve_slide(slide_id)
    osr = openslide.OpenSlide(str(path))
    dz = DeepZoomGenerator(osr, tile_size=TILE_SIZE, overlap=OVERLAP)
    with _cache_lock:
        _cache[slide_id] = (osr, dz)
    return osr, dz


class TileHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, data: bytes, content_type: str, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=3600")
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status: int, msg: str):
        body = json.dumps({"error": msg}).encode()
        self._send(body, "application/json", status)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        qs = parse_qs(parsed.query)

        # Health — no auth
        if path == "" or path == "/":
            self._send(b'{"status":"ok"}', "application/json")
            return

        # Auth
        ok, err = _check_auth(self.headers)
        if not ok:
            self._error(401, err)
            return

        # List source slides
        if path == "/slides":
            _rebuild_index()
            items = []
            for sid in sorted(_slide_index):
                fpath = _slide_index[sid]
                items.append({"id": sid, "filename": fpath.name, "size_bytes": fpath.stat().st_size, "type": "source"})
            self._send(json.dumps(items, indent=2).encode(), "application/json")
            return

        # List stain outputs
        if path == "/stains":
            _rebuild_index()
            # Group by job_id
            jobs: dict[str, list] = {}
            for sid, fpath in sorted(_stain_index.items()):
                parts = sid.split("__")
                job_id = parts[1] if len(parts) >= 2 else "unknown"
                if job_id not in jobs:
                    jobs[job_id] = []
                jobs[job_id].append({
                    "slide_id": sid,
                    "filename": fpath.name,
                    "size_bytes": fpath.stat().st_size,
                })
            result = [{"job_id": jid, "files": files} for jid, files in sorted(jobs.items())]
            self._send(json.dumps(result, indent=2).encode(), "application/json")
            return

        # List files in a specific stain job
        m = re.match(r"^/stains/([^/]+)$", path)
        if m:
            job_id = m.group(1)
            _rebuild_index()
            files = []
            for sid, fpath in _stain_index.items():
                if f"__{job_id}__" in sid:
                    files.append({
                        "slide_id": sid,
                        "filename": fpath.name,
                        "size_bytes": fpath.stat().st_size,
                    })
            if not files:
                self._error(404, f"No stain outputs for job: {job_id}")
                return
            self._send(json.dumps(files, indent=2).encode(), "application/json")
            return

        # All slides (source + stains)
        if path == "/all":
            _rebuild_index()
            items = []
            for sid, fpath in sorted(_slide_index.items()):
                items.append({"id": sid, "filename": fpath.name, "size_bytes": fpath.stat().st_size, "type": "source"})
            for sid, fpath in sorted(_stain_index.items()):
                items.append({"id": sid, "filename": fpath.name, "size_bytes": fpath.stat().st_size, "type": "stain"})
            self._send(json.dumps(items, indent=2).encode(), "application/json")
            return

        # DZI descriptor: /slides/{id}.dzi
        m = re.match(r"^/slides/([^/]+)\.dzi$", path)
        if m:
            slide_id = m.group(1)
            try:
                _, dz = _get_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            self._send(dz.get_dzi("jpeg").encode(), "application/xml")
            return

        # Tile: /slides/{id}/{level}/{col}_{row}.jpeg
        m = re.match(r"^/slides/([^/]+)/(\d+)/(\d+)_(\d+)\.jpeg$", path)
        if m:
            slide_id, level, col, row = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
            try:
                _, dz = _get_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            try:
                tile = dz.get_tile(level, (col, row))
            except (ValueError, openslide.OpenSlideError) as e:
                self._error(400, str(e))
                return
            buf = io.BytesIO()
            tile.save(buf, format="JPEG", quality=JPEG_QUALITY)
            self._send(buf.getvalue(), "image/jpeg")
            return

        # Slide info: /slides/{id}/info
        m = re.match(r"^/slides/([^/]+)/info$", path)
        if m:
            slide_id = m.group(1)
            try:
                osr, dz = _get_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            # Determine type
            _rebuild_index()
            slide_type = "source" if slide_id in _slide_index else "stain"
            info = {
                "id": slide_id,
                "type": slide_type,
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
            self._send(json.dumps(info, indent=2).encode(), "application/json")
            return

        # Thumbnail: /slides/{id}/thumbnail?max_size=512
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

        # Full file download: /slides/{id}/download
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
                while True:
                    chunk = f.read(1024 * 1024)  # 1MB chunks
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            return

        # Region download: /slides/{id}/region?x=0&y=0&w=1024&h=1024&level=0
        m = re.match(r"^/slides/([^/]+)/region$", path)
        if m:
            slide_id = m.group(1)
            try:
                osr, _ = _get_slide(slide_id)
            except KeyError:
                self._error(404, f"Slide not found: {slide_id}")
                return
            try:
                x = int(qs.get("x", [0])[0])
                y = int(qs.get("y", [0])[0])
                w = int(qs.get("w", [1024])[0])
                h = int(qs.get("h", [1024])[0])
                level = int(qs.get("level", [0])[0])
                fmt = qs.get("format", ["jpeg"])[0].lower()
            except (ValueError, IndexError):
                self._error(400, "Invalid region parameters")
                return
            # Clamp dimensions
            max_dim = 8192
            w = min(w, max_dim)
            h = min(h, max_dim)
            level = min(level, osr.level_count - 1)
            try:
                region = osr.read_region((x, y), level, (w, h)).convert("RGB")
            except Exception as e:
                self._error(400, f"Region read failed: {e}")
                return
            buf = io.BytesIO()
            if fmt == "png":
                region.save(buf, format="PNG")
                ct = "image/png"
            else:
                region.save(buf, format="JPEG", quality=JPEG_QUALITY)
                ct = "image/jpeg"
            self._send(buf.getvalue(), ct)
            return

        self._error(404, "Not found")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()


def main():
    print(f"Tileserver starting on :{PORT}")
    print(f"  SLIDE_DIR:    {SLIDE_DIR}")
    print(f"  STAIN_DIR:    {STAIN_DIR}")
    print(f"  TILE_SIZE:    {TILE_SIZE}")
    print(f"  OVERLAP:      {OVERLAP}")
    print(f"  JPEG_QUALITY: {JPEG_QUALITY}")

    _rebuild_index()
    print(f"  Source slides: {len(_slide_index)}")
    print(f"  Stain outputs: {len(_stain_index)}")

    server = HTTPServer(("0.0.0.0", PORT), TileHandler)
    print(f"Ready: http://0.0.0.0:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
