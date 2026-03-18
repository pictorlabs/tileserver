"""Minimal OpenSlide tile server for whole-slide images.

Serves DeepZoom-compatible tiles for integration with OpenSeadragon.

Endpoints:
    GET /                          → health check (no auth)
    GET /slides                    → list available slides
    GET /slides/{slide_id}.dzi     → DeepZoom descriptor (XML)
    GET /slides/{slide_id}/{level}/{col}_{row}.jpeg  → tile
    GET /slides/{slide_id}/info    → slide metadata (dimensions, mpp, levels)
    GET /slides/{slide_id}/thumbnail?max_size=512  → thumbnail

Environment:
    SLIDE_DIR    → directory containing .svs/.tiff/.ndpi/.mrxs files (default: /data)
    SERVE_PORT   → listen port (default: 8080)
    TILE_SIZE    → tile size in pixels (default: 254)
    OVERLAP      → tile overlap in pixels (default: 1)
    AUTH0_DOMAIN → Auth0 tenant domain (e.g. https://dev.us.auth0.com). If unset, auth is disabled.
    AUTH0_AUDIENCE → expected JWT audience
"""

import io
import json
import os
import re
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen
from xml.etree.ElementTree import Element, SubElement, tostring

import openslide
from openslide.deepzoom import DeepZoomGenerator
from PIL import Image

SLIDE_DIR = Path(os.environ.get("SLIDE_DIR", "/data"))
PORT = int(os.environ.get("SERVE_PORT", os.environ.get("PORT", "8080")))
TILE_SIZE = int(os.environ.get("TILE_SIZE", "254"))
OVERLAP = int(os.environ.get("OVERLAP", "1"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "80"))

# Auth0 config — if AUTH0_DOMAIN is unset, auth is disabled (open access)
AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN", "")
AUTH0_AUDIENCE = os.environ.get("AUTH0_AUDIENCE", "")

# ── Minimal JWT validation (no external deps beyond stdlib) ──
_jwks_cache: dict | None = None
_jwks_cache_time: float = 0
JWKS_CACHE_TTL = 3600

def _get_jwks() -> dict:
    """Fetch JWKS from Auth0, with caching."""
    global _jwks_cache, _jwks_cache_time
    if _jwks_cache and time.time() - _jwks_cache_time < JWKS_CACHE_TTL:
        return _jwks_cache
    url = f"{AUTH0_DOMAIN.rstrip('/')}/.well-known/jwks.json"
    with urlopen(Request(url), timeout=10) as resp:
        _jwks_cache = json.loads(resp.read())
        _jwks_cache_time = time.time()
        return _jwks_cache

def _b64url_decode(s: str) -> bytes:
    """Base64url decode (no padding)."""
    import base64
    s += '=' * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s)

def _verify_jwt(token: str) -> dict | None:
    """Verify an RS256 JWT against Auth0 JWKS. Returns payload or None."""
    try:
        import jwt as pyjwt
        jwks = _get_jwks()
        header = pyjwt.get_unverified_header(token)
        for key in jwks.get("keys", []):
            if key.get("kid") == header.get("kid"):
                public_key = pyjwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key))
                payload = pyjwt.decode(
                    token,
                    public_key,
                    algorithms=["RS256"],
                    audience=AUTH0_AUDIENCE,
                    issuer=f"{AUTH0_DOMAIN.rstrip('/')}/",
                )
                return payload
        return None
    except Exception:
        return None

def _check_auth(headers) -> tuple[bool, str]:
    """Check Authorization header. Returns (ok, error_message)."""
    if not AUTH0_DOMAIN:
        return True, ""  # Auth disabled
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False, "Missing or invalid Authorization header"
    token = auth.split(" ", 1)[1]
    payload = _verify_jwt(token)
    if payload is None:
        return False, "Invalid or expired token"
    return True, ""

# Cache: slide_id → (OpenSlide, DeepZoomGenerator)
_cache: dict[str, tuple[openslide.OpenSlide, DeepZoomGenerator]] = {}

SUPPORTED_EXT = {".svs", ".tiff", ".tif", ".ndpi", ".mrxs", ".scn", ".bif", ".vms"}


def _scan_slides() -> dict[str, Path]:
    """Scan SLIDE_DIR for supported slide files. Returns {slide_id: path}."""
    slides = {}
    for f in SLIDE_DIR.iterdir():
        if f.suffix.lower() in SUPPORTED_EXT and f.is_file():
            slides[f.stem] = f
    return slides


def _get_slide(slide_id: str) -> tuple[openslide.OpenSlide, DeepZoomGenerator]:
    """Get or open a slide + its DeepZoom generator."""
    if slide_id in _cache:
        return _cache[slide_id]
    slides = _scan_slides()
    if slide_id not in slides:
        raise KeyError(f"Slide not found: {slide_id}")
    osr = openslide.OpenSlide(str(slides[slide_id]))
    dz = DeepZoomGenerator(osr, tile_size=TILE_SIZE, overlap=OVERLAP)
    _cache[slide_id] = (osr, dz)
    return osr, dz


class TileHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # quiet

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

        # Health — no auth (Knative readiness probe)
        if path == "" or path == "/":
            self._send(b'{"status":"ok"}', "application/json")
            return

        # Auth check on all other endpoints
        ok, err = _check_auth(self.headers)
        if not ok:
            self._error(401, err)
            return

        # List slides
        if path == "/slides":
            slides = _scan_slides()
            items = []
            for sid, fpath in sorted(slides.items()):
                items.append({"id": sid, "filename": fpath.name, "size_bytes": fpath.stat().st_size})
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
            info = {
                "id": slide_id,
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
    print(f"  TILE_SIZE:    {TILE_SIZE}")
    print(f"  OVERLAP:      {OVERLAP}")
    print(f"  JPEG_QUALITY: {JPEG_QUALITY}")
    slides = _scan_slides()
    print(f"  Slides found: {len(slides)}")
    for sid in sorted(slides):
        print(f"    - {sid}")
    server = HTTPServer(("0.0.0.0", PORT), TileHandler)
    print(f"Ready: http://0.0.0.0:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
