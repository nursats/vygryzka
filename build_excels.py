import os
import json
import datetime
import openpyxl

JSON_DIR = "json_responses"
OUTPUT_TAX = "1_Налоговые_отчисления.xlsx"
OUTPUT_FOT = "2_Оценочный_ФОТ.xlsx"
OUTPUT_FINES = "3_Штрафы_и_пени.xlsx"
MAX_EXCEL_ROWS = 1_048_576
MAX_DATA_ROWS_PER_SHEET = MAX_EXCEL_ROWS - 1


def timestamp_to_date(ts):
    if ts is None:
        return ""
    try:
        return datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return ""


def load_all_payments():
    """Load all payments from saved JSONs."""
    all_payments = []
    json_files = sorted(f for f in os.listdir(JSON_DIR) if f.endswith(".json"))
    print(f"Found {len(json_files)} JSON files")

    for jf in json_files:
        identifier = jf.replace(".json", "")
        try:
            with open(os.path.join(JSON_DIR, jf), "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            print(f"  Skipping corrupted file {jf}: {e}")
            continue

        payments = data.get("content", {}).get("answer", {}).get("payment", [])
        for p in payments:
            all_payments.append({
                "identifier": identifier,
                "summa": p.get("summa", 0) or 0,
                "receipt_date": timestamp_to_date(p.get("receiptDate")),
                "write_off_date": timestamp_to_date(p.get("writeOffDate")),
                "year": p.get("year", ""),
                "pay_type": p.get("payType", ""),
                "tax_org_code": p.get("taxOrgCode", ""),
                "name_ru": p.get("nameTaxRu", ""),
                "name_kz": p.get("nameTaxKz", ""),
                "kbk_code": p.get("kbk", ""),
                "kbk_name_ru": p.get("kbkNameRu", ""),
                "kbk_name_kz": p.get("kbkNameKz", ""),
            })

    print(f"Total payment records: {len(all_payments)}")
    return all_payments


def append_rows_with_sheet_split(workbook, sheet_title, headers, rows):
    sheet_index = 1
    ws = workbook.active
    ws.title = sheet_title
    ws.append(headers)
    written_on_sheet = 0

    for row in rows:
        if written_on_sheet >= MAX_DATA_ROWS_PER_SHEET:
            sheet_index += 1
            ws = workbook.create_sheet(f"{sheet_title} {sheet_index}")
            ws.append(headers)
            written_on_sheet = 0

        ws.append(row)
        written_on_sheet += 1


def build_tax_excel(payments):
    """File 1: All tax payments broken down by KBK."""
    print(f"\nBuilding {OUTPUT_TAX}...")
    wb = openpyxl.Workbook()
    headers = [
        "identifier", "summa", "receipt_date", "write_off_date", "year",
        "pay_type", "tax_org_code", "name_ru", "name_kz",
        "kbk_code", "name_ru", "name_kz"
    ]

    rows = []
    for p in payments:
        rows.append([
            p["identifier"], p["summa"], p["receipt_date"], p["write_off_date"],
            p["year"], p["pay_type"], p["tax_org_code"], p["name_ru"], p["name_kz"],
            p["kbk_code"], p["kbk_name_ru"], p["kbk_name_kz"],
        ])
    append_rows_with_sheet_split(wb, "Налоговые отчисления", headers, rows)

    wb.save(OUTPUT_TAX)
    print(f"  Saved {OUTPUT_TAX}: {len(rows)} rows")


def build_fot_excel(payments):
    """File 2: Estimated payroll fund (FOT).
    Logic from FotService: filter kbk_code == '901101', summa * 10.
    """
    print(f"\nBuilding {OUTPUT_FOT}...")
    wb = openpyxl.Workbook()
    headers = ["identifier", "summa", "write_off_date", "year"]
    rows = []
    for p in payments:
        if p["kbk_code"] == "901101" and p["pay_type"] == 1:
            fot_summa = round(p["summa"] * 10, 2)
            rows.append([
                p["identifier"], fot_summa, p["write_off_date"], p["year"]
            ])
    append_rows_with_sheet_split(wb, "Оценочный ФОТ", headers, rows)

    wb.save(OUTPUT_FOT)
    print(f"  Saved {OUTPUT_FOT}: {len(rows)} rows")


def build_fines_excel(payments):
    """File 3: Fines and penalties.
    Logic from TaxService: pay_type in (2, 3) — пени и штрафы.
    """
    print(f"\nBuilding {OUTPUT_FINES}...")
    wb = openpyxl.Workbook()
    headers = [
        "identifier", "summa", "receipt_date", "write_off_date", "year",
        "pay_type", "tax_org_code", "name_ru", "name_kz",
        "kbk_code", "name_ru", "name_kz"
    ]
    rows = []
    for p in payments:
        if p["pay_type"] in (2, 3):
            rows.append([
                p["identifier"], p["summa"], p["receipt_date"], p["write_off_date"],
                p["year"], p["pay_type"], p["tax_org_code"], p["name_ru"], p["name_kz"],
                p["kbk_code"], p["kbk_name_ru"], p["kbk_name_kz"],
            ])
    append_rows_with_sheet_split(wb, "Штрафы и пени", headers, rows)

    wb.save(OUTPUT_FINES)
    print(f"  Saved {OUTPUT_FINES}: {len(rows)} rows")


def main():
    payments = load_all_payments()
    build_tax_excel(payments)
    build_fot_excel(payments)
    build_fines_excel(payments)
    print("\nDone! All 3 Excel files created.")


if __name__ == "__main__":
    main()
