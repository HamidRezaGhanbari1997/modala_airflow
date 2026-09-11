from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path

import jdatetime
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from mahidasht.auth import get_token

CONFIG_PATH = Path(__file__).with_name("config.json")

# واترمارک برای همه‌ی provider ها مشترک است (کلید provider+service_id)، اما
# دیتای واقعی هر provider توی جدول جدای خودش ذخیره میشه - ر.ک. stock_table_name.
WATERMARK_TABLE = "mahidasht_watermark"


def stock_table_name(provider: str) -> str:
    safe_provider = re.sub(r"[^a-z0-9_]", "", provider.strip().lower())
    if not safe_provider:
        raise ValueError(f"Invalid provider name for table routing: {provider!r}")
    return f"mahidasht_stock_{safe_provider}"

# JSON field (from the mahidasht API) -> destination column name
FIELD_MAP = {
    "dcCode": "dc_code",
    "dcName": "dc_name",
    "branchCode": "branch_code",
    "branchName": "branch_name",
    "storeCode": "store_code",
    "storeName": "store_name",
    "goodsCode": "goods_code",
    "supplierSystemCode": "supplier_system_code",
    "goodsName": "goods_name",
    "sstid": "sstid",
    "volume": "volume",
    "weightUnit": "weight_unit",
    "weight": "weight",
    "unity": "unity",
    "brandCode": "brand_code",
    "brandName": "brand_name",
    "supplierCode": "supplier_code",
    "supplierName": "supplier_name",
    "stockDate": "stock_date",
    "preStock": "pre_stock",
    "inStock": "in_stock",
    "outStock": "out_stock",
    "stock": "stock",
    "reserve": "reserve",
    "remainStock": "remain_stock",
    "lastBuyPrice": "last_buy_price",
    "lastCost": "last_cost",
    "lastSalePrice": "last_sale_price",
    "lastConsumerPrice": "last_consumer_price",
}


def load_providers_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


_database_ensured = False


def _mssql_env() -> tuple[str, str, str, str, str, str]:
    host = os.environ["MSSQL_HOST"]
    port = os.environ.get("MSSQL_PORT", "1433")
    database = os.environ["MSSQL_DB"]
    user = os.environ["MSSQL_USER"]
    password = os.environ["MSSQL_PASSWORD"]
    driver = os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    return host, port, database, user, password, driver


def _odbc_engine(database: str) -> Engine:
    host, port, _database, user, password, driver = _mssql_env()
    odbc_str = (
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
        f"UID={user};PWD={password};TrustServerCertificate=yes;Encrypt=yes"
    )
    conn_str = urllib.parse.quote_plus(odbc_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}", fast_executemany=True)


def ensure_database_exists() -> None:
    # Same self-healing pattern as ensure_watermark_table_exists /
    # ensure_stock_table_exists below, one level up: the destination
    # database itself (MSSQL_DB, normally ats_staging) is never
    # provisioned anywhere else, so a fresh SQL Server instance would
    # otherwise need it created by hand before any DAG could run. Runs at
    # most once per worker process (memoized) against the always-present
    # `master` database, since the target database may not exist yet.
    global _database_ensured
    if _database_ensured:
        return

    _, _, database, _, _, _ = _mssql_env()
    master_engine = _odbc_engine("master")
    try:
        with master_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(
                text("SELECT 1 FROM sys.databases WHERE name = :db"), {"db": database}
            ).scalar()
            if not exists:
                conn.execute(text(f"CREATE DATABASE [{database}]"))
    finally:
        master_engine.dispose()

    _database_ensured = True


def get_engine() -> Engine:
    ensure_database_exists()
    _, _, database, _, _, _ = _mssql_env()
    return _odbc_engine(database)


def ensure_watermark_table_exists(engine: Engine) -> None:
    # One row per (provider, service_id, last_fetched_date) - a full day-by-day
    # log of every date ever attempted, success or failed, not just a single
    # "current position" row per agent. response_body carries the service's
    # error/response text when last_status='failed', for debugging.
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '{WATERMARK_TABLE}')
                BEGIN
                    CREATE TABLE dbo.{WATERMARK_TABLE} (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        provider VARCHAR(100) NOT NULL,
                        service_id INT NOT NULL,
                        last_fetched_date DATE NULL,
                        last_fetched_date_jalali VARCHAR(10) NULL,
                        last_status VARCHAR(20) NULL,
                        last_row_count INT NULL,
                        response_body NVARCHAR(MAX) NULL,
                        duration_seconds FLOAT NULL,
                        updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                        CONSTRAINT UQ_{WATERMARK_TABLE}_provider_service_date UNIQUE (provider, service_id, last_fetched_date)
                    );
                END
                """
            )
        )

        # Migration for a table created before this became a per-day log:
        # drop the old one-row-per-agent unique constraint and add the column.
        conn.execute(
            text(
                f"""
                IF EXISTS (
                    SELECT 1 FROM sys.key_constraints
                    WHERE name = 'UQ_{WATERMARK_TABLE}_provider_service'
                      AND parent_object_id = OBJECT_ID('dbo.{WATERMARK_TABLE}')
                )
                BEGIN
                    ALTER TABLE dbo.{WATERMARK_TABLE} DROP CONSTRAINT UQ_{WATERMARK_TABLE}_provider_service;
                END
                """
            )
        )
        conn.execute(
            text(
                f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID('dbo.{WATERMARK_TABLE}') AND name = 'response_body'
                )
                BEGIN
                    ALTER TABLE dbo.{WATERMARK_TABLE} ADD response_body NVARCHAR(MAX) NULL;
                END
                """
            )
        )
        conn.execute(
            text(
                f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.columns
                    WHERE object_id = OBJECT_ID('dbo.{WATERMARK_TABLE}') AND name = 'duration_seconds'
                )
                BEGIN
                    ALTER TABLE dbo.{WATERMARK_TABLE} ADD duration_seconds FLOAT NULL;
                END
                """
            )
        )
        conn.execute(
            text(
                f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.key_constraints
                    WHERE name = 'UQ_{WATERMARK_TABLE}_provider_service_date'
                      AND parent_object_id = OBJECT_ID('dbo.{WATERMARK_TABLE}')
                )
                BEGIN
                    ALTER TABLE dbo.{WATERMARK_TABLE}
                        ADD CONSTRAINT UQ_{WATERMARK_TABLE}_provider_service_date
                        UNIQUE (provider, service_id, last_fetched_date);
                END
                """
            )
        )


def ensure_stock_table_exists(engine: Engine, provider: str) -> str:
    table = stock_table_name(provider)

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '{table}')
                BEGIN
                    CREATE TABLE dbo.{table} (
                        id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        provider VARCHAR(100) NOT NULL,
                        service_id INT NOT NULL,
                        service_name NVARCHAR(255) NULL,
                        fetch_date DATE NOT NULL,
                        fetch_date_jalali VARCHAR(10) NOT NULL,
                        dc_code INT NULL,
                        dc_name NVARCHAR(255) NULL,
                        branch_code INT NULL,
                        branch_name NVARCHAR(255) NULL,
                        store_code INT NULL,
                        store_name NVARCHAR(255) NULL,
                        goods_code VARCHAR(50) NULL,
                        supplier_system_code VARCHAR(50) NULL,
                        goods_name NVARCHAR(500) NULL,
                        sstid VARCHAR(50) NULL,
                        volume FLOAT NULL,
                        weight_unit FLOAT NULL,
                        weight FLOAT NULL,
                        unity FLOAT NULL,
                        brand_code VARCHAR(50) NULL,
                        brand_name NVARCHAR(255) NULL,
                        supplier_code VARCHAR(50) NULL,
                        supplier_name NVARCHAR(255) NULL,
                        stock_date DATE NULL,
                        pre_stock FLOAT NULL,
                        in_stock FLOAT NULL,
                        out_stock FLOAT NULL,
                        stock FLOAT NULL,
                        reserve FLOAT NULL,
                        remain_stock FLOAT NULL,
                        last_buy_price FLOAT NULL,
                        last_cost FLOAT NULL,
                        last_sale_price FLOAT NULL,
                        last_consumer_price FLOAT NULL,
                        inserted_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
                    );
                    CREATE INDEX IX_{table}_service_date
                        ON dbo.{table} (service_id, fetch_date);
                END
                """
            )
        )

    return table


def to_jalali_str(d: date) -> str:
    return jdatetime.date.fromgregorian(date=d).strftime("%Y/%m/%d")


def from_jalali_str(jalali_str: str) -> date:
    year, month, day = (int(part) for part in jalali_str.strip().split("/"))
    return jdatetime.date(year, month, day).togregorian()


def get_last_success_date(engine: Engine, provider: str, service_id: int) -> date | None:
    # Only status='success': the resume point is the last day that actually
    # produced data, so a run always retries forward from real progress -
    # see compute_date_range for how failed days get retried (capped).
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT MAX(last_fetched_date) FROM dbo.{WATERMARK_TABLE}
                WHERE provider = :provider AND service_id = :service_id AND last_status = 'success'
                """
            ),
            {"provider": provider, "service_id": service_id},
        ).fetchone()
    return row[0] if row and row[0] else None


def compute_date_range(engine: Engine, provider: str, service_id: int) -> tuple[date, date]:
    offset_days = int(os.environ.get("MAHIDASHT_WATERMARK_END_OFFSET_DAYS", "2"))
    end_date = date.today() - timedelta(days=offset_days)

    last_success = get_last_success_date(engine, provider, service_id)

    if last_success is None:
        # Never succeeded even once - a brand new agent, do a full backfill
        # from the configured start date. Not capped by the retry window
        # below: that window only protects against re-walking a long *failed*
        # streak after some real progress, not an intentional first backfill.
        initial_start = os.environ.get("MAHIDASHT_INITIAL_START_DATE", "").strip()
        start_date = from_jalali_str(initial_start) if initial_start else end_date
        return start_date, end_date

    naive_start = last_success + timedelta(days=1)

    # If a provider's server has been down for a long stretch (e.g. Vision
    # hanging on every request for 2+ months), resuming strictly from
    # last_success+1 would re-attempt every single one of those failed days
    # (each up to ~125s) on every DAG run, forever, without ever reaching
    # today's data. Cap how far back we're willing to retry failed days;
    # anything older than the window is left as a permanent "failed" log
    # entry and simply skipped so newer data always gets a chance.
    retry_window_days = int(os.environ.get("MAHIDASHT_FAILED_RETRY_WINDOW_DAYS", "7"))
    retry_floor = end_date - timedelta(days=retry_window_days - 1)

    start_date = max(naive_start, retry_floor)
    return start_date, end_date


def upsert_watermark(
    engine: Engine,
    provider: str,
    service_id: int,
    fetched_date: date,
    fetched_date_jalali: str,
    status: str,
    row_count: int,
    response_body: str | None = None,
    duration_seconds: float | None = None,
) -> None:
    # response_body: the service's error/response text, kept only when
    # status='failed' so a later review can see why a given day was skipped.
    # duration_seconds: wall-clock time spent fetching+loading that one day.
    if response_body is not None and len(response_body) > 4000:
        response_body = response_body[:4000]

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                MERGE dbo.{WATERMARK_TABLE} AS target
                USING (
                    SELECT :provider AS provider, :service_id AS service_id, :fetched_date AS fetched_date
                ) AS src
                ON target.provider = src.provider
                   AND target.service_id = src.service_id
                   AND target.last_fetched_date = src.fetched_date
                WHEN MATCHED THEN UPDATE SET
                    last_fetched_date_jalali = :fetched_date_jalali,
                    last_status = :status,
                    last_row_count = :row_count,
                    response_body = :response_body,
                    duration_seconds = :duration_seconds,
                    updated_at = SYSDATETIME()
                WHEN NOT MATCHED THEN INSERT
                    (provider, service_id, last_fetched_date, last_fetched_date_jalali, last_status, last_row_count, response_body, duration_seconds)
                    VALUES (:provider, :service_id, :fetched_date, :fetched_date_jalali, :status, :row_count, :response_body, :duration_seconds);
                """
            ),
            {
                "provider": provider,
                "service_id": service_id,
                "fetched_date": fetched_date,
                "fetched_date_jalali": fetched_date_jalali,
                "status": status,
                "row_count": row_count,
                "response_body": response_body,
                "duration_seconds": duration_seconds,
            },
        )


def call_api(service_id: int, provider: str, jalali_date: str) -> tuple[dict | None, str | None]:
    # MAHIDASHT_BRIDGE_BASE_URL is the mahidasht_bridge FastAPI service itself
    # (unauthenticated, root-level routes); MAHIDASHT_API_BASE_URL is kept as
    # a fallback for older setups where the two were the same host. The
    # Authorization header below is still sent via get_token() against
    # MAHIDASHT_API_BASE_URL for whatever gateway ends up in front of the
    # bridge later - mahidasht_bridge itself ignores it today.
    base_url = os.environ.get(
        "MAHIDASHT_BRIDGE_BASE_URL", os.environ.get("MAHIDASHT_API_BASE_URL", "")
    ).rstrip("/")
    url = f"{base_url}/fetch/agent/{service_id}"
    timeout = int(os.environ.get("MAHIDASHT_REQUEST_TIMEOUT_SECONDS", "60"))

    def _post(token: str) -> requests.Response:
        return requests.post(
            url,
            json={"date": jalali_date, "providers": [provider]},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    try:
        response = _post(get_token())
        if response.status_code == 401:
            # token expired/invalid mid-run (or a stale cache) - force a fresh login and retry once
            response = _post(get_token(force_refresh=True))
    except requests.RequestException as exc:
        error = f"request failed: {exc}"
        print(f"[mahidasht] {error} for service_id={service_id} date={jalali_date}")
        return None, error

    if response.status_code != 200:
        error = f"non-200 response: {response.status_code} {response.text[:2000]}"
        print(f"[mahidasht] {error} for service_id={service_id} date={jalali_date}")
        return None, error

    try:
        payload = response.json()
    except ValueError:
        error = f"invalid JSON response: {response.text[:2000]}"
        print(f"[mahidasht] {error} for service_id={service_id} date={jalali_date}")
        return None, error

    if payload.get("status") != "success":
        error = f"status != success: {json.dumps(payload)[:2000]}"
        print(f"[mahidasht] {error} for service_id={service_id} date={jalali_date}")
        return None, error

    return payload, None


def load_rows(
    engine: Engine,
    table: str,
    provider: str,
    service_id: int,
    service_name: str | None,
    fetch_date: date,
    fetch_date_jalali: str,
    rows: list[dict],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                DELETE FROM dbo.{table}
                WHERE service_id = :service_id AND fetch_date = :fetch_date
                """
            ),
            {"service_id": service_id, "fetch_date": fetch_date},
        )

    if not rows:
        return

    records = []
    for row in rows:
        record = {dest: row.get(src) for src, dest in FIELD_MAP.items()}
        record["provider"] = provider
        record["service_id"] = service_id
        record["service_name"] = service_name
        record["fetch_date"] = fetch_date
        record["fetch_date_jalali"] = fetch_date_jalali
        records.append(record)

    columns = list(records[0].keys())
    column_list = ", ".join(columns)
    placeholder_list = ", ".join(f":{c}" for c in columns)

    with engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO dbo.{table} ({column_list}) VALUES ({placeholder_list})"),
            records,
        )


def fetch_and_load_service(provider: str, service_id: int, **_context) -> None:
    engine = get_engine()
    ensure_watermark_table_exists(engine)
    table = ensure_stock_table_exists(engine, provider)

    start_date, end_date = compute_date_range(engine, provider, service_id)
    if start_date > end_date:
        print(f"[mahidasht] nothing to do for provider={provider} service_id={service_id}: "
              f"start={start_date} end={end_date}")
        return

    current = start_date
    while current <= end_date:
        jalali_str = to_jalali_str(current)
        print(f"[mahidasht] fetching provider={provider} service_id={service_id} date={jalali_str}")
        day_started_at = time.perf_counter()

        payload, error = call_api(service_id, provider, jalali_str)
        if payload is None:
            # One inline retry to absorb a transient blip (a slow upstream
            # response, a bridge restart mid-run) without marking a day
            # "failed" that would have succeeded a few seconds later.
            print(f"[mahidasht] retrying once provider={provider} service_id={service_id} date={jalali_str}")
            time.sleep(5)
            payload, error = call_api(service_id, provider, jalali_str)

        if payload is None:
            # Still failing - this is a permanent problem for this specific
            # date (e.g. bad data on the provider's own server), not a
            # transient outage. Recording it as "failed" (with the service's
            # response for later review) and moving on keeps one bad day from
            # blocking every later day forever, since the next run's
            # start_date is always last_fetched_date + 1.
            print(
                f"[mahidasht] permanently failed provider={provider} service_id={service_id} "
                f"date={jalali_str} - marking as failed and continuing to the next day"
            )
            duration_seconds = time.perf_counter() - day_started_at
            upsert_watermark(
                engine, provider, service_id, current, jalali_str, "failed", 0,
                response_body=error, duration_seconds=duration_seconds,
            )
            current += timedelta(days=1)
            continue

        rows = payload.get("data", []) or []
        load_rows(engine, table, provider, service_id, payload.get("name"), current, jalali_str, rows)
        duration_seconds = time.perf_counter() - day_started_at
        upsert_watermark(
            engine, provider, service_id, current, jalali_str, "success", len(rows),
            duration_seconds=duration_seconds,
        )

        print(
            f"[mahidasht] loaded {len(rows)} rows for provider={provider} service_id={service_id} "
            f"date={jalali_str} in {duration_seconds:.2f}s"
        )
        current += timedelta(days=1)
