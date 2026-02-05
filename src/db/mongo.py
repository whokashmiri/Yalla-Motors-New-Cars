from __future__ import annotations

import os
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient

_client: Optional[AsyncIOMotorClient] = None

def _mongo_url() -> str:
    url = os.getenv("MONGO_URL", "").strip()
    if not url:
        raise RuntimeError("MONGO_URL is required (set it in .env)")
    return url

def _mongo_db() -> str:
    name = os.getenv("MONGO_DB", "").strip()
    if not name:
        raise RuntimeError("MONGO_DB is required (set it in .env)")
    return name

def get_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(_mongo_url())
    return _client

def get_db():
    return get_client()[_mongo_db()]

def get_collection_name() -> str:
    return os.getenv("MONGO_COLLECTION", "yalla_new_cars").strip() or "yalla_new_cars"
