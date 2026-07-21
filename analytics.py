from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError

LOGGER = logging.getLogger("tabletop.analytics")


class AnalyticsStore:
    """Best-effort analytics writer; telemetry must never block gameplay."""

    def __init__(self) -> None:
        self._uri = os.getenv("MONGODB_URI", "").strip()
        self._database_name = os.getenv("MONGODB_DATABASE", "tabletop")
        self._retention_days = max(1, int(os.getenv("ANALYTICS_RETENTION_DAYS", "180")))
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=5000)
        self._client: MongoClient[dict[str, Any]] | None = None
        self._worker: asyncio.Task[None] | None = None

    @property
    def enabled(self) -> bool:
        return bool(self._uri)

    @property
    def status(self) -> str:
        if not self.enabled:
            return "disabled"
        return "connected" if self._client is not None else "unavailable"

    async def start(self) -> None:
        if not self.enabled:
            LOGGER.info("MongoDB analytics disabled: MONGODB_URI is not set")
            return
        try:
            self._client = MongoClient(
                self._uri,
                serverSelectionTimeoutMS=3000,
                connectTimeoutMS=3000,
                appname="tabletop-server",
            )
            await asyncio.to_thread(self._prepare_database)
        except PyMongoError:
            LOGGER.exception("MongoDB analytics initialization failed")
            if self._client is not None:
                self._client.close()
            self._client = None
            return
        self._worker = asyncio.create_task(self._run(), name="analytics-writer")
        LOGGER.info("MongoDB analytics enabled")

    async def stop(self) -> None:
        if self._worker is not None:
            await self._queue.join()
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            self._client.close()

    def record(self, document: dict[str, Any]) -> bool:
        if self._client is None:
            return False
        document = dict(document)
        document.setdefault("occurred_at", datetime.now(timezone.utc))
        document["received_at"] = datetime.now(timezone.utc)
        try:
            self._queue.put_nowait(document)
            return True
        except asyncio.QueueFull:
            LOGGER.warning("Analytics queue full; dropping event")
            return False

    def _prepare_database(self) -> None:
        assert self._client is not None
        self._client.admin.command("ping")
        collection = self._client[self._database_name]["events"]
        collection.create_index([("event_name", ASCENDING), ("received_at", DESCENDING)])
        collection.create_index([("session_id", ASCENDING), ("received_at", DESCENDING)])
        collection.create_index(
            "expires_at", expireAfterSeconds=0, name="event_retention_ttl"
        )

    async def _run(self) -> None:
        while True:
            first = await self._queue.get()
            batch = [first]
            while len(batch) < 100:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            expires_at = datetime.now(timezone.utc) + timedelta(days=self._retention_days)
            for document in batch:
                document["expires_at"] = expires_at
            try:
                assert self._client is not None
                collection = self._client[self._database_name]["events"]
                await asyncio.to_thread(collection.insert_many, batch, ordered=False)
            except PyMongoError:
                LOGGER.exception("MongoDB analytics batch write failed")
            finally:
                for _ in batch:
                    self._queue.task_done()
