import datetime
import json
import re
from pathlib import Path
from typing import List, Sequence

import openpyxl

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
BIN_LIST_PATH = ROOT_DIR / "all_bins.txt"
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


def normalize_identifier(value):
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 11:
        digits = f"0{digits}"
    if len(digits) != 12:
        return None
    return digits


def read_identifiers(path: Path) -> List[str]:
    raw_identifiers = re.findall(r"\d+", path.read_text(encoding="utf-8"))
    identifiers = []
    seen = set()

    for raw_identifier in raw_identifiers:
        identifier = normalize_identifier(raw_identifier)
        if identifier is None or identifier in seen:
            continue
        identifiers.append(identifier)
        seen.add(identifier)

    return identifiers


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


def build_empty_row(identifier: str):
    return [
        identifier,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]


def load_rows(identifiers: Sequence[str]):
    rows = []
    print(f"Found identifiers in all_bins.txt: {len(identifiers)}")

    for identifier in identifiers:
        path = JSON_DIR / f"{identifier}.json"
        if not path.exists():
            rows.append(build_empty_row(identifier))
            continue

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"Skipping {path.name}: {e}")
            rows.append(build_empty_row(identifier))
            continue

        rows.append(
            [
                identifier,
                normalize_cell(data.get("name", "")),
                normalize_cell(data.get("fullName", "")),
                normalize_cell(data.get("rnn", "")),
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
    identifiers = read_identifiers(BIN_LIST_PATH)
    rows = load_rows(identifiers)

    wb = openpyxl.Workbook()
    append_rows_with_sheet_split(wb, "basic_info", headers, rows)
    wb.save(OUTPUT_FILE)

    print(f"Saved {OUTPUT_FILE}: {len(rows)} rows")


if __name__ == "__main__":
    main()
