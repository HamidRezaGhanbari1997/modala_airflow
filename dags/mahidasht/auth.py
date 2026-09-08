from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests

CACHE_DIR = Path(__file__).with_name(".cache")
TOKEN_CACHE_PATH = CACHE_DIR / "token.json"

# server-side token TTL is 24h; refresh a bit early to avoid edge-of-expiry 401s
TOKEN_TTL_SECONDS = 23 * 60 * 60


def _login() -> dict:
    base_url = os.environ["MAHIDASHT_API_BASE_URL"].rstrip("/")
    username = os.environ["MAHIDASHT_API_USERNAME"]
    password = os.environ["MAHIDASHT_API_PASSWORD"]
    timeout = int(os.environ.get("MAHIDASHT_REQUEST_TIMEOUT_SECONDS", "60"))

    response = requests.post(
        f"{base_url}/auth/login",
        json={"username": username, "password": password},
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()

    cached = {"access_token": payload["access_token"], "obtained_at": time.time()}
    CACHE_DIR.mkdir(exist_ok=True)
    with open(TOKEN_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cached, f)

    return cached


def _read_cache() -> dict | None:
    if not TOKEN_CACHE_PATH.exists():
        return None
    try:
        with open(TOKEN_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (ValueError, OSError):
        return None


def get_token(force_refresh: bool = False) -> str:
    """Return a valid bearer token, logging in and caching to disk as needed.

    Tokens expire server-side after 24h, so they're minted on demand via
    /auth/login rather than kept as a static secret in .env.
    """
    if not force_refresh:
        cached = _read_cache()
        if cached and (time.time() - cached["obtained_at"]) < TOKEN_TTL_SECONDS:
            return cached["access_token"]

    return _login()["access_token"]
