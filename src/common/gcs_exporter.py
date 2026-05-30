"""
Batched GCS log exporter — buffers events in memory and flushes to GCS
as newline-delimited JSON every FLUSH_INTERVAL seconds or when the
batch reaches BATCH_SIZE events.
"""
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

try:
    from google.cloud import storage
    _GCS_AVAILABLE = True
except ImportError:
    _GCS_AVAILABLE = False

BUCKET = os.getenv("GCS_LOG_BUCKET", "").replace("gs://", "").split("/")[0]
FLUSH_INTERVAL = int(os.getenv("GCS_FLUSH_INTERVAL", "60"))
BATCH_SIZE = int(os.getenv("GCS_BATCH_SIZE", "100"))


class GCSExporter:
    def __init__(self, service: str, log_prefix: str):
        self.service = service
        self.log_prefix = log_prefix  # e.g. "pipeline-logs/genai"
        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._client: Optional[object] = None
        self._enabled = _GCS_AVAILABLE and bool(BUCKET)

        if self._enabled:
            try:
                self._client = storage.Client()
                self._bucket = self._client.bucket(BUCKET)
                self._start_flush_thread()
            except Exception as e:
                print(f"[GCSExporter] disabled — {e}")
                self._enabled = False

    def emit(self, event: dict) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._buffer.append(event)
            if len(self._buffer) >= BATCH_SIZE:
                self._flush_locked()

    def _start_flush_thread(self) -> None:
        t = threading.Thread(target=self._flush_loop, daemon=True)
        t.start()

    def _flush_loop(self) -> None:
        while True:
            time.sleep(FLUSH_INTERVAL)
            with self._lock:
                self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer[:]
        self._buffer.clear()
        try:
            ts = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
            blob_name = f"{self.log_prefix}/{ts}-{self.service}.json"
            blob = self._bucket.blob(blob_name)
            ndjson = "\n".join(json.dumps(e) for e in batch)
            blob.upload_from_string(ndjson, content_type="application/json")
        except Exception as e:
            print(f"[GCSExporter] flush failed — {e}")

    def flush(self) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._flush_locked()
