import asyncio
import json
import re
import time
from pathlib import Path
from typing import List

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
BIN_LIST_PATH = ROOT_DIR / "all_bins.txt"
API_TOKEN = "G1BCW0TvPlFFf7jM7wmLoi"
API_URL = "https://kompra.kz/api/v2/basic"
JSON_DIR = SCRIPT_DIR / "json"
PROGRESS_FILE = SCRIPT_DIR / "progress_basic_info.json"
MAX_RETRIES = 2
RETRY_DELAY = 2
REQUEST_TIMEOUT = 5
CONCURRENCY = 10


def load_progress():
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {"completed": []}

        if isinstance(data, dict) and isinstance(data.get("completed"), list):
            return data
    return {"completed": []}


def save_progress(progress):
    PROGRESS_FILE.write_text(
        json.dumps(progress, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_identifier(value: str):
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 11:
        digits = f"0{digits}"
    if len(digits) != 12:
        return None
    return digits


def read_identifiers(path: Path) -> List[str]:
    try:
        raw_identifiers = re.findall(r"\d+", path.read_text(encoding="utf-8"))
    except OSError:
        return []

    identifiers = []
    seen = set()
    for raw_identifier in raw_identifiers:
        identifier = normalize_identifier(raw_identifier)
        if identifier is None or identifier in seen:
            continue
        identifiers.append(identifier)
        seen.add(identifier)
    return identifiers


def fetch_basic_sync(identifier: str):
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                API_URL,
                params={"identifier": identifier, "api-token": API_TOKEN},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    Request error for {identifier}: {e}, retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"    Failed after {MAX_RETRIES} attempts for {identifier}: {e}")
                return None
    return None


async def fetch_basic(identifier: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        data = await asyncio.to_thread(fetch_basic_sync, identifier)
        return identifier, data


async def main():
    JSON_DIR.mkdir(parents=True, exist_ok=True)

    progress = load_progress()
    completed = set(progress.get("completed", []))

    identifiers = read_identifiers(BIN_LIST_PATH)
    remaining = [identifier for identifier in identifiers if identifier not in completed]

    print(f"Loaded identifiers from {BIN_LIST_PATH.name}: {len(identifiers)}")
    print(f"Already completed: {len(completed)}")
    print(f"Remaining: {len(remaining)}")
    print(f"Output folder: {JSON_DIR}")
    print(f"Request timeout: {REQUEST_TIMEOUT}s")
    print(f"Concurrency: {CONCURRENCY}")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [asyncio.create_task(fetch_basic(identifier, semaphore)) for identifier in remaining]

    processed = len(completed)
    for task in asyncio.as_completed(tasks):
        identifier, data = await task
        print(f"[{processed + 1}/{len(identifiers)}] Processed {identifier}")

        if data is None:
            print(f"    Skipped {identifier} (request failed), will retry next run")
            processed += 1
            continue

        json_path = JSON_DIR / f"{identifier}.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        progress.setdefault("completed", []).append(identifier)
        completed.add(identifier)
        save_progress(progress)
        processed += 1

    print(f"\nFinished. JSON files are in: {JSON_DIR}")


if __name__ == "__main__":
    asyncio.run(main())
