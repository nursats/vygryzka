import datetime
import json
from pathlib import Path
from typing import Sequence

import openpyxl

SCRIPT_DIR = Path(__file__).resolve().parent
JSON_DIR = SCRIPT_DIR / "json"
OUTPUT_FILE = SCRIPT_DIR / "basic_info.xlsx"
MAX_EXCEL_ROWS = 1_048_576
MAX_DATA_ROWS_PER_SHEET = MAX_EXCEL_ROWS - 1


def timestamp_to_date(value):
    if value in (None, ""):
        return ""
    try:
        return datetime.datetime.fromtimestamp(value / 1000).strftime("%Y-%m-%d")
    except (ValueError, OSError, TypeError):
        return ""


def normalize_cell(value):
    if value in (None, ""):
        return ""
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        normalized_items = [normalize_cell(item) for item in value if item not in (None, "")]
        return "; ".join(str(item) for item in normalized_items if item != "")
    return value


def stringify_list(value):
    if isinstance(value, list):
        return normalize_cell(value)
    return normalize_cell(value)


def append_rows_with_sheet_split(workbook, sheet_title: str, headers: Sequence[str], rows: Sequence[Sequence]):
    sheet_index = 1
    ws = workbook.active
    ws.title = sheet_title
    ws.append(list(headers))
    written_on_sheet = 0

    for row in rows:
        if written_on_sheet >= MAX_DATA_ROWS_PER_SHEET:
            sheet_index += 1
            ws = workbook.create_sheet(f"{sheet_title} {sheet_index}")
            ws.append(list(headers))
            written_on_sheet = 0

        ws.append(list(row))
        written_on_sheet += 1


def load_rows():
    rows = []
    json_files = sorted(JSON_DIR.glob("*.json"))
    print(f"Found JSON files: {len(json_files)}")

    for path in json_files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"Skipping {path.name}: {e}")
            continue

        rows.append(
            [
                normalize_cell(data.get("bin", path.stem)),
                normalize_cell(data.get("name", "")),
                normalize_cell(data.get("fullName", "")),
                normalize_cell(data.get("rnn", "")),
                normalize_cell(data.get("iin", "")),
                stringify_list(data.get("field")),
                normalize_cell(data.get("factAddress", "")),
                normalize_cell(data.get("region", "")),
                normalize_cell(data.get("lawAddress", "")),
                normalize_cell(data.get("okpo", "")),
                normalize_cell(data.get("oked", "")),
                normalize_cell(data.get("owner", "")),
                normalize_cell(data.get("ownerIin", "")),
                timestamp_to_date(data.get("registerDate")),
                normalize_cell(data.get("workers", "")),
                normalize_cell(data.get("size", "")),
                normalize_cell(data.get("krpCode", "")),
                normalize_cell(data.get("ownership", "")),
                normalize_cell(data.get("kato", "")),
                normalize_cell(data.get("city", "")),
                normalize_cell(data.get("street", "")),
                normalize_cell(data.get("secondaryOked", "")),
                normalize_cell(data.get("kbe", "")),
                normalize_cell(data.get("phone", "")),
                normalize_cell(data.get("email", "")),
                normalize_cell(data.get("website", "")),
                normalize_cell(data.get("stateInvolvement", "")),
                normalize_cell(data.get("astanaHub", "")),
            ]
        )

    return rows


def main():
    if not JSON_DIR.exists():
        raise FileNotFoundError(f"JSON folder not found: {JSON_DIR}")

    headers = [
        "bin",
        "name",
        "fullName",
        "rnn",
        "iin",
        "field",
        "factAddress",
        "region",
        "lawAddress",
        "okpo",
        "oked",
        "owner",
        "ownerIin",
        "registerDate",
        "workers",
        "size",
        "krpCode",
        "ownership",
        "kato",
        "city",
        "street",
        "secondaryOked",
        "kbe",
        "phone",
        "email",
        "website",
        "stateInvolvement",
        "astanaHub",
    ]
    rows = load_rows()

    wb = openpyxl.Workbook()
    append_rows_with_sheet_split(wb, "basic_info", headers, rows)
    wb.save(OUTPUT_FILE)

    print(f"Saved {OUTPUT_FILE}: {len(rows)} rows")


if __name__ == "__main__":
    main()
