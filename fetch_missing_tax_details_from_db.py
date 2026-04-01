import datetime
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

import psycopg2
import psycopg2.extras

DB_HOST = "10.0.0.1"
DB_PORT = 5432
DB_USER = "nursat"
DB_PASSWORD = "wMscCE367z7W"
DB_NAME = "kompra"

BIN_LIST_PATH = Path("all_bins.txt")
PROGRESS_FILE = Path("progress_all_bins.json")
JSON_DIR = Path("all_bins_json")


def load_progress():
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {"completed": []}

        if isinstance(data, dict) and isinstance(data.get("completed"), list):
            return data

    return {"completed": []}


def normalize_identifier(value: str) -> Optional[str]:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 11:
        digits = f"0{digits}"
    if len(digits) != 12:
        return None
    return digits


def read_identifiers(path: Path) -> List[str]:
    raw_text = path.read_text(encoding="utf-8")
    raw_identifiers = re.findall(r"\d+", raw_text)
    identifiers = []
    seen = set()

    for raw_identifier in raw_identifiers:
        identifier = normalize_identifier(raw_identifier)
        if identifier is None or identifier in seen:
            continue
        identifiers.append(identifier)
        seen.add(identifier)

    return identifiers


def datetime_to_timestamp_ms(value):
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        value = datetime.datetime.combine(value, datetime.time.min)
    if value.tzinfo is None:
        return int(value.timestamp() * 1000)
    return int(value.astimezone(datetime.timezone.utc).timestamp() * 1000)


def normalize_scalar(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def build_empty_response(bin_id: str, company=None):
    return {
        "content": {
            "answer": {
                "payment": [],
                "nameRu": normalize_scalar(company["name"]) if company else None,
                "nameKz": None,
                "iin_BIN": bin_id,
            }
        },
        "status": False,
        "answer": None,
    }


def build_response(bin_id: str, company, rows):
    payments = []
    for row in rows:
        payments.append(
            {
                "id": normalize_scalar(row["id"]),
                "summa": normalize_scalar(row["summa"]) if row["summa"] is not None else 0,
                "receiptDate": datetime_to_timestamp_ms(row["receipt_date"]),
                "writeOffDate": datetime_to_timestamp_ms(row["write_off_date"]),
                "year": normalize_scalar(row["year"]),
                "payType": normalize_scalar(row["pay_type"]),
                "taxOrgCode": normalize_scalar(row["tax_org_code"]),
                "nameTaxRu": normalize_scalar(row["name_tax_ru"]),
                "nameTaxKz": normalize_scalar(row["name_tax_kz"]),
                "kbk": normalize_scalar(row["kbk"]),
                "kbkNameRu": normalize_scalar(row["kbk_name_ru"]),
                "kbkNameKz": normalize_scalar(row["kbk_name_kz"]),
                "entryType": normalize_scalar(row["entry_type"]),
                "payNum": normalize_scalar(row["pay_num"]),
                "created": datetime_to_timestamp_ms(row["created"]),
                "lastUpdated": datetime_to_timestamp_ms(row["last_updated"]),
                "companyId": normalize_scalar(row["company_id"]),
            }
        )

    return {
        "content": {
            "answer": {
                "payment": payments,
                "nameRu": normalize_scalar(company["name"]) if company else None,
                "nameKz": None,
                "iin_BIN": bin_id,
            }
        },
        "status": True,
        "answer": None,
    }


def fetch_company(cursor, bin_id: str):
    cursor.execute(
        """
        select
            id,
            bin,
            name
        from company
        where bin = %s
        """,
        (bin_id,),
    )
    return cursor.fetchone()


def fetch_latest_tax_details(cursor, company_id: int):
    cursor.execute(
        """
        with latest_snapshot as (
            select max(last_updated) as last_updated
            from v2_tax_details
            where company_id = %s
        )
        select
            id,
            company_id,
            entry_type,
            kbk,
            kbk_name_ru,
            kbk_name_kz,
            name_tax_kz,
            name_tax_ru,
            pay_num,
            summa,
            tax_org_code,
            receipt_date,
            write_off_date,
            year,
            created,
            last_updated,
            pay_type
        from v2_tax_details
        where company_id = %s
          and last_updated = (select last_updated from latest_snapshot)
        order by id
        """,
        (company_id, company_id),
    )
    return cursor.fetchall()


def main():
    JSON_DIR.mkdir(parents=True, exist_ok=True)

    identifiers = read_identifiers(BIN_LIST_PATH)
    completed = set(load_progress().get("completed", []))
    missing = [identifier for identifier in identifiers if identifier not in completed]

    print(f"Loaded identifiers: {len(identifiers)}")
    print(f"Already completed via API: {len(completed)}")
    print(f"Missing to backfill from DB: {len(missing)}")

    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
    )

    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            for index, bin_id in enumerate(missing, start=1):
                print(f"[{index}/{len(missing)}] Backfilling {bin_id}")

                company = fetch_company(cursor, bin_id)
                if company is None:
                    payload = build_empty_response(bin_id)
                else:
                    rows = fetch_latest_tax_details(cursor, company["id"])
                    if rows:
                        payload = build_response(bin_id, company, rows)
                    else:
                        payload = build_empty_response(bin_id, company)

                output_path = JSON_DIR / f"{bin_id}.json"
                output_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
    finally:
        connection.close()

    print(f"Finished. Backfilled JSON files are in: {JSON_DIR}")


if __name__ == "__main__":
    main()
