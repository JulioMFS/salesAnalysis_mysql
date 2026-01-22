import os
import pandas as pd
from db import get_connection


def parse_euro_amount(value):
    if pd.isna(value) or value == "":
        return None

    value = (
        str(value)
        .replace("€", "")
        .replace("\u00a0", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", ".")
        .strip()
    )

    try:
        amount = float(value)

        # Divide by 1000 if value looks inflated
        if abs(amount) > 1000:
            amount = amount / 1000

        return amount

    except ValueError:
        return None


def import_sales_excels(folder_path):
    results = []
    conn = get_connection()
    cursor = conn.cursor()

    for filename in os.listdir(folder_path):

        # ✅ HARD FILTER
        if (
            filename.startswith("~$")
            or not filename.startswith("Vendas")
            or not filename.lower().endswith(".xlsx")
        ):
            continue

        file_path = os.path.join(folder_path, filename)

        try:
            df = pd.read_excel(file_path, dtype=str)
        except Exception as e:
            results.append({
                "file": filename,
                "status": "error",
                "message": f"Failed to read Excel: {e}"
            })
            continue

        df = df.fillna("")
        rows_to_insert = []

        for _, row in df.iterrows():
            # Column B → Date
            sale_date = pd.to_datetime(row.iloc[1], dayfirst=True, errors="coerce")
            if pd.isna(sale_date):
                continue

            # Column C → Payment method
            payment_method = row.iloc[2].strip()
            if not payment_method:
                continue

            # Column D → Amount
            amount = parse_euro_amount(row.iloc[3])
            if amount is None:
                continue

            rows_to_insert.append(
                (sale_date.date(), payment_method, amount)
            )

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

def import_single_sales_excel(file_path):
    folder = os.path.dirname(file_path)
    filename = os.path.basename(file_path)

    results = import_sales_excels(folder)

    for r in results:
        if r.get("file") == filename:
            return r

    return {
        "file": filename,
        "status": "error",
        "message": "File not processed"
    }
