from __future__ import annotations

import os
from datetime import datetime

import requests
from airflow.decorators import dag, task


API_BASE_URL = os.getenv("ATS_API_BASE_URL", "http://host.docker.internal:8000")
TARGETS_URL = f"{API_BASE_URL}/targets"
EXTRACT_URL = f"{API_BASE_URL}/extract"
REQUEST_TIMEOUT_SECONDS = 30
EXTRACT_TIMEOUT_SECONDS = 60


def _resolve_target_key(target: dict) -> str | None:
    configured_target_key = target.get("target_key")
    if configured_target_key:
        return configured_target_key

    override_target_key = os.getenv("ATS_TARGET_KEY_OVERRIDE")
    if override_target_key:
        return override_target_key

    excel_path = target.get("excel_path") or ""
    normalized_path = excel_path.replace("\\", "/")
    file_name = os.path.basename(normalized_path).lower()
    fallback_map = {
        "budget.xlsx": "target",
        "موجودی ذرت جنوب.xlsx".lower(): "corn_availability",
    }
    return fallback_map.get(file_name)


def _safe_json(response: requests.Response) -> dict | list | str:
    try:
        return response.json()
    except ValueError:
        return response.text


@dag(
    dag_id="excel_api_extraction_loop",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["excel", "api", "docker"],
)
def excel_api_extraction_loop():
    @task
    def fetch_sheet_jobs() -> list[dict]:
        try:
            targets_response = requests.get(TARGETS_URL, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            return [
                {
                    "target_key": None,
                    "sheet_name": None,
                    "status_code": None,
                    "success": False,
                    "response": None,
                    "error": f"Failed to access /targets: {exc}",
                    "skip": True,
                }
            ]

        if targets_response.status_code != 200:
            return [
                {
                    "target_key": None,
                    "sheet_name": None,
                    "status_code": targets_response.status_code,
                    "success": False,
                    "response": _safe_json(targets_response),
                    "error": "Failed to fetch target list from /targets",
                    "skip": True,
                }
            ]

        try:
            payload = targets_response.json()
        except ValueError:
            payload = {"targets": []}

        targets = payload.get("targets", [])
        jobs: list[dict] = []

        for target in targets:
            target_key = _resolve_target_key(target)
            sheets = target.get("sheets", []) or []

            if not target_key:
                jobs.append(
                    {
                        "target_key": None,
                        "sheet_name": None,
                        "status_code": None,
                        "success": False,
                        "response": None,
                        "error": "No usable target key was found for this target",
                        "skip": True,
                    }
                )
                continue

            if not sheets:
                jobs.append(
                    {
                        "target_key": target_key,
                        "sheet_name": None,
                        "status_code": None,
                        "success": False,
                        "response": None,
                        "error": "No sheets were registered for this target",
                        "skip": True,
                    }
                )
                continue

            for sheet_name in sheets:
                jobs.append(
                    {
                        "target_key": target_key,
                        "sheet_name": sheet_name,
                        "status_code": None,
                        "success": False,
                        "response": None,
                        "error": None,
                        "skip": False,
                    }
                )

        return jobs

    @task
    def run_sheet_extraction(job: dict) -> dict:
        if job.get("skip"):
            return job

        try:
            extract_response = requests.post(
                EXTRACT_URL,
                json={"target_key": job["target_key"], "sheet_name": job["sheet_name"]},
                timeout=EXTRACT_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            return {
                **job,
                "status_code": None,
                "success": False,
                "response": None,
                "error": f"Extraction request failed: {exc}",
            }

        return {
            **job,
            "status_code": extract_response.status_code,
            "success": extract_response.status_code == 200,
            "response": _safe_json(extract_response),
            "error": None if extract_response.status_code == 200 else "Extraction request failed",
        }

    @task
    def summarize_results(results: list[dict]) -> dict:
        success_count = sum(1 for item in results if item.get("success"))
        failed_count = len(results) - success_count

        for item in results:
            print(
                f"target={item.get('target_key')} sheet={item.get('sheet_name')} "
                f"status={item.get('status_code')} success={item.get('success')}"
            )

        return {
            "total_requests": len(results),
            "success_count": success_count,
            "failed_count": failed_count,
            "results": results,
        }

    sheet_jobs = fetch_sheet_jobs()
    extracted_results = run_sheet_extraction.expand(job=sheet_jobs)
    summarize_results(extracted_results)


excel_api_extraction_loop()
