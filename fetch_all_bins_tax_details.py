import asyncio
import json
import os
import re
import time
from pathlib import Path
from typing import List

import requests

BIN_LIST_PATH = "all_bins.txt"
API_TOKEN = "G1BCW0TvPlFFf7jM7wmLoi"
API_URL = "https://kompra.kz/api/v2/tax-details"
JSON_DIR = "all_bins_json"
PROGRESS_FILE = "progress_all_bins.json"
MAX_RETRIES = 2
RETRY_DELAY = 2
REQUEST_TIMEOUT = 5
CONCURRENCY = 10


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {"completed": []}

        if isinstance(data, dict) and isinstance(data.get("completed"), list):
            return data
    return {"completed": []}


def save_progress(progress):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def read_identifiers(path: str) -> List[str]:
    text = Path(path).read_text(encoding="utf-8")
    raw_identifiers = re.findall(r"\d+", text)
    identifiers = []
    seen = set()

    for raw_identifier in raw_identifiers:
        identifier = normalize_identifier(raw_identifier)
        if identifier is None or identifier in seen:
            continue
        identifiers.append(identifier)
        seen.add(identifier)

    return identifiers


def normalize_identifier(value: str):
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 11:
        digits = f"0{digits}"
    if len(digits) != 12:
        return None
    return digits


def fetch_tax_details_sync(identifier: str):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                API_URL,
                params={"identifier": identifier, "api-token": API_TOKEN},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            top_status = data.get("status")
            payments = data.get("content", {}).get("answer", {}).get("payment", [])

            if len(payments) == 0 and top_status is None:
                if attempt < MAX_RETRIES - 1:
                    print(
                        f"    Empty response (status=null) for {identifier}, "
                        f"retrying in {RETRY_DELAY}s (attempt {attempt + 1}/{MAX_RETRIES})..."
                    )
                    time.sleep(RETRY_DELAY)
                    continue

                print(f"    Max retries reached for {identifier}, skipping")
                return None

            if len(payments) == 0 and top_status is False:
                print(f"    No tax data for {identifier} (status=false), saving")

            return data

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    Request error for {identifier}: {e}, retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"    Failed after {MAX_RETRIES} attempts for {identifier}: {e}")
                return None

    return None


async def fetch_tax_details(identifier: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        return await asyncio.to_thread(fetch_tax_details_sync, identifier)


async def fetch_one(identifier: str, semaphore: asyncio.Semaphore):
    data = await fetch_tax_details(identifier, semaphore)
    return identifier, data


async def main():
    os.makedirs(JSON_DIR, exist_ok=True)

    progress = load_progress()
    completed = set(progress.get("completed", []))

    identifiers = read_identifiers(BIN_LIST_PATH)
    remaining = [identifier for identifier in identifiers if identifier not in completed]

    print(f"Loaded identifiers from {BIN_LIST_PATH}: {len(identifiers)}")
    print(f"Already completed: {len(completed)}")
    print(f"Remaining: {len(remaining)}")
    print(f"Output folder: {JSON_DIR}")
    print(f"Request timeout: {REQUEST_TIMEOUT}s")
    print(f"Concurrency: {CONCURRENCY}")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [asyncio.create_task(fetch_one(identifier, semaphore)) for identifier in remaining]

    processed = len(completed)
    for task in asyncio.as_completed(tasks):
        identifier, data = await task
        print(f"[{processed + 1}/{len(identifiers)}] Processed {identifier}")
        if data is None:
            print(f"    Skipped {identifier} (no data), will retry next run")
            processed += 1
            continue

        json_path = os.path.join(JSON_DIR, f"{identifier}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        progress.setdefault("completed", []).append(identifier)
        completed.add(identifier)
        save_progress(progress)
        processed += 1

    print(f"\nFinished. JSON files are in: {JSON_DIR}")


if __name__ == "__main__":
    for i in range(3):
        asyncio.run(main())
