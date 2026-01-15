import os
import pandas as pd
from db import get_connection


def parse_euro_amount(value):
    if pd.isna(value):
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
        return float(value)
    except ValueError:
        return None


def import_sales_excels(folder_path):
    conn = get_connection()
    cursor = conn.cursor()
    total_rows = 0

    for filename in os.listdir(folder_path):
        if filename.startswith("~$") or not filename.lower().endswith((".xls", ".xlsx")):
            continue

        file_path = os.path.join(folder_path, filename)

        # -------- Read Excel --------
        try:
            df = pd.read_excel(file_path, dtype=str)
        except Exception as e:
            print(f"❌ Failed to read {filename}: {e}")
            continue

        df = df.fillna("")

        # -------- Parse column B for dates --------
        df["sale_date"] = pd.to_datetime(df.iloc[:, 1], dayfirst=True, errors="coerce")

        # -------- Prepare amounts --------
        df["amount_q"] = df.iloc[:, 16].apply(parse_euro_amount)  # Column Q
        df["amount_r"] = df.iloc[:, 17].apply(parse_euro_amount)  # Column R

        # -------- Active date propagation --------
        current_date = None
        rows_to_insert = []

        for i, row in df.iterrows():
            if pd.notna(row["sale_date"]):
                current_date = row["sale_date"].date()

            if current_date is None:
                continue  # Skip rows before the first date

            amount = None
            # Column Q if L is empty
            if row.iloc[11] == "":
                amount = row["amount_q"]
            # Column R if M is empty
            elif row.iloc[12] == "":
                amount = row["amount_r"]

            if pd.notna(amount):
                rows_to_insert.append((current_date, amount))
                current_date = None

        if not rows_to_insert:
            print(f"⚠️ No valid sales rows found in {filename}")
            continue

        min_date = min(r[0] for r in rows_to_insert)
        max_date = max(r[0] for r in rows_to_insert)

        # -------- Delete overlapping rows --------
        try:
            cursor.execute(
                "DELETE FROM sales WHERE source_file=%s AND sale_date BETWEEN %s AND %s",
                (filename, min_date, max_date),
            )

            # -------- Insert / Update --------
            for sale_date, amount in rows_to_insert:
                cursor.execute(
                    """
                    INSERT INTO sales (sale_date, amount, source_file)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        amount = VALUES(amount),
                        source_file = VALUES(source_file)
                    """,
                    (sale_date, amount, filename),
                )

            conn.commit()
            total_rows += len(rows_to_insert)
            print(f"✔ Imported {len(rows_to_insert)} rows from {filename} ({min_date} → {max_date})")

        except Exception as e:
            conn.rollback()
            print(f"❌ Failed to import {filename}: {e}")

    print(f"\nTotal sales rows imported/updated: {total_rows}")
    cursor.close()
    conn.close()
