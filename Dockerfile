FROM python:3.12-slim@sha256:3d5ed973e45820f5ba5e46bd065bd88b3a504ff0724d85980dcd05eab361fcf4

RUN apt-get update && \
    apt-get install -y --no-install-recommends libopenslide0 && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir openslide-python Pillow "PyJWT[crypto]>=2.8"

WORKDIR /app
COPY serve.py .

EXPOSE 8080
ENTRYPOINT ["python", "serve.py"]
