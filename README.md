# tileserver

Minimal OpenSlide tile server for whole-slide images. Serves DeepZoom-compatible tiles for OpenSeadragon integration.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health check |
| GET | `/slides` | List available slides |
| GET | `/slides/{id}.dzi` | DeepZoom descriptor (XML) |
| GET | `/slides/{id}/{level}/{col}_{row}.jpeg` | Tile |
| GET | `/slides/{id}/info` | Slide metadata |
| GET | `/slides/{id}/thumbnail?max_size=512` | Thumbnail |

## Run locally

```bash
SLIDE_DIR=/path/to/slides uv run python serve.py
```

## Docker

```bash
docker build -t tileserver .
docker run -p 8080:8080 -v /path/to/slides:/data tileserver
```

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `SLIDE_DIR` | `/data` | Directory containing slide files |
| `PORT` | `8080` | Listen port |
| `TILE_SIZE` | `254` | Tile size in pixels |
| `OVERLAP` | `1` | Tile overlap in pixels |
| `JPEG_QUALITY` | `80` | JPEG compression quality |

## Supported formats

`.svs`, `.tiff`, `.tif`, `.ndpi`, `.mrxs`, `.scn`, `.bif`, `.vms`
