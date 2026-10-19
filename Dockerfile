FROM python:3.14-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml uv.lock ./
COPY src ./src
COPY THIRD_PARTY_NOTICES.md ./
RUN pip install --no-cache-dir uv==0.13.1 \
    && uv sync --frozen --no-dev --extra media --extra embedding --extra drive \
    && mkdir -m 0700 /data && chown 10001:10001 /data
USER 10001:10001
ENV MEMENTO_DATA_DIR=/data HF_HOME=/data/models OMP_NUM_THREADS=2
EXPOSE 8765
HEALTHCHECK --interval=30s --timeout=5s --start-period=180s --retries=3 \
    CMD ["/app/.venv/bin/python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8765/health', timeout=3)"]
ENTRYPOINT ["/app/.venv/bin/memento"]
CMD ["--host", "0.0.0.0"]
