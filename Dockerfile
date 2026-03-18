FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopenslide0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml .
RUN uv python install 3.12 && uv sync --no-dev

COPY serve.py .

EXPOSE 8080
ENTRYPOINT ["uv", "run", "python", "serve.py"]
