import os
import re
import pdfplumber
import pandas as pd
from db import get_connection


def parse_pt_amount(value):
    if value is None:
        return None

    value = str(value)
    value = value.replace("€", "").replace("\xa0", "").strip()

    if value == "":
        return None

    negative = value.startswith("-")
    if negative:
        value = value[1:]

    value = value.replace(" ", "").replace(".", "").replace(",", ".")

    try:
        amount = float(value)
        return -amount if negative else amount
    except ValueError:
        return None


def import_sales_pdfs(folder_path):
    results = []
    conn = get_connection()
    cursor = conn.cursor()

    for filename in os.listdir(folder_path):

        # ✅ HARD FILTER (adjust if needed)
        if (
            not filename.lower().endswith(".pdf")
            or not filename.startswith("Vendas")
        ):
            continue

        file_path = os.path.join(folder_path, filename)
        rows_to_insert = []

        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    for line in text.split("\n"):
                        # Match lines like:
                        # 1 01-12-2025 Dinheiro 542,420€
                        match = re.match(
                            r"\d+\s+"
                            r"(\d{2}-\d{2}-\d{4})\s+"
                            r"(.+?)\s+"
                            r"([\d\s.,]+)€",
                            line
                        )

                        if not match:
                            continue

                        sale_date_raw, payment_method, amount_raw = match.groups()

                        sale_date = pd.to_datetime(
                            sale_date_raw,
                            dayfirst=True,
                            errors="coerce"
                        )

                        if pd.isna(sale_date):
                            continue

                        amount = parse_pt_amount(amount_raw)
                        if amount is None:
                            continue

                        rows_to_insert.append(
                            (sale_date.date(), payment_method.strip(), amount)
                        )

        except Exception as e:
            results.append({
                "file": filename,
                "status": "error",
                "message": f"Failed to read PDF: {e}"
            })
            continue

        if not rows_to_insert:
            results.append({
                "file": filename,
                "status": "error",
                "message": "No valid sales rows"
            })
            continue

        min_date = min(r[0] for r in rows_to_insert)
        max_date = max(r[0] for r in rows_to_insert)

        try:
            for sale_date, payment_method, amount in rows_to_insert:
                cursor.execute(
                    """
                    INSERT INTO sales (sale_date, payment_method, amount)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        amount = VALUES(amount)
                    """,
                    (sale_date, payment_method, amount),
                )

            conn.commit()

            results.append({
                "file": filename,
                "status": "ok",
                "rows": len(rows_to_insert),
                "min_date": min_date.isoformat(),
                "max_date": max_date.isoformat(),
            })

        except Exception as e:
            conn.rollback()
            results.append({
                "file": filename,
                "status": "error",
                "message": str(e)
            })

    cursor.close()
    conn.close()
    return results


def import_single_sales_pdf(file_path):
    folder = os.path.dirname(file_path)
    filename = os.path.basename(file_path)

    results = import_sales_pdfs(folder)

    for r in results:
        if r.get("file") == filename:
            return r

    return {
        "file": filename,
        "status": "error",
        "message": "File not processed"
    }
