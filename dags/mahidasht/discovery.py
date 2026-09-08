from __future__ import annotations

import json
import os
from pathlib import Path

import requests

CACHE_DIR = Path(__file__).with_name(".cache")


def _cache_path(provider: str) -> Path:
    return CACHE_DIR / f"{provider}_service_ids.json"


def _read_cache(provider: str) -> list[int]:
    path = _cache_path(provider)
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (ValueError, OSError):
        return []


def _write_cache(provider: str, service_ids: list[int]) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    with open(_cache_path(provider), "w", encoding="utf-8") as f:
        json.dump(service_ids, f)


def discover_service_ids(provider: str) -> list[int]:
    """Fetch the live list of service_ids for a provider from the mahidasht
    availability endpoint. Falls back to the last cached result (written to
    disk on a previous successful call) if the API is unreachable at DAG
    parse time, so a temporary outage doesn't take down DAG parsing.
    """
    # MAHIDASHT_BRIDGE_BASE_URL is the mahidasht_bridge FastAPI service itself
    # (unauthenticated, root-level routes); MAHIDASHT_API_BASE_URL is kept as
    # a fallback for older setups where the two were the same host.
    base_url = os.environ.get(
        "MAHIDASHT_BRIDGE_BASE_URL", os.environ.get("MAHIDASHT_API_BASE_URL", "")
    ).rstrip("/")
    if not base_url:
        return _read_cache(provider)

    timeout = int(os.environ.get("MAHIDASHT_DISCOVERY_TIMEOUT_SECONDS", "10"))
    url = f"{base_url}/providers/availability"

    try:
        response = requests.get(
            url,
            params={"providers": provider},
            headers={"Accept": "application/json"},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"[mahidasht] discovery failed for provider={provider}, falling back to cache: {exc}")
        return _read_cache(provider)

    service_ids = sorted(
        {item["service_id"] for item in payload.get("providers", []) if "service_id" in item}
    )

    if service_ids:
        _write_cache(provider, service_ids)
        return service_ids

    print(f"[mahidasht] discovery returned no service_ids for provider={provider}, falling back to cache")
    return _read_cache(provider)
