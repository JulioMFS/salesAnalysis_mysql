import os
import pandas as pd
from db import get_connection

def parse_euro_amount(value):
    if pd.isna(value):
        return None
    value = str(value).replace("€", "").replace("\u00a0", "").replace(" ", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(value)
    except ValueError:
        return None

def import_sales_excels(folder_path):
    results = []
    conn = get_connection()
    cursor = conn.cursor()

    for filename in os.listdir(folder_path):
        if filename.startswith("~$") or not filename.lower().endswith((".xls", ".xlsx")):
            continue

        file_path = os.path.join(folder_path, filename)

        try:
            df = pd.read_excel(file_path, dtype=str)
        except Exception as e:
            results.append({"file": filename, "status": "error", "message": f"Failed to read Excel: {e}"})
            continue

        df = df.fillna("")

        # -------- Parse column B for dates --------
        if df.shape[1] > 1:
            df["sale_date"] = pd.to_datetime(df.iloc[:, 1], dayfirst=True, errors="coerce")
        else:
            df["sale_date"] = None

        # -------- Prepare amounts safely --------
        df["amount_q"] = df.iloc[:, 16].apply(parse_euro_amount) if df.shape[1] > 16 else None
        df["amount_r"] = df.iloc[:, 17].apply(parse_euro_amount) if df.shape[1] > 17 else None

        current_date = None
        rows_to_insert = []

        for _, row in df.iterrows():
            if pd.notna(row.get("sale_date")):
                current_date = row["sale_date"].date()
            if current_date is None:
                continue  # Skip rows before first date

            amount = None
            # Column Q if L (index 11) is empty
            if df.shape[1] > 11 and row.iloc[11] == "":
                amount = row["amount_q"]
            # Column R if M (index 12) is empty
            elif df.shape[1] > 12 and row.iloc[12] == "":
                amount = row["amount_r"]

            if pd.notna(amount):
                rows_to_insert.append((current_date, amount))
                current_date = None

        if not rows_to_insert:
            results.append({"file": filename, "status": "error", "message": "No valid sales rows"})
            continue

        min_date = min(r[0] for r in rows_to_insert)
        max_date = max(r[0] for r in rows_to_insert)

        try:
            cursor.execute(
                "DELETE FROM sales WHERE source_file=%s AND sale_date BETWEEN %s AND %s",
                (filename, min_date, max_date),
            )
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

            results.append({
                "file": filename,
                "status": "ok",
                "message": f"{len(rows_to_insert)} rows imported",
                "rows": len(rows_to_insert),
                "min_date": min_date.isoformat(),
                "max_date": max_date.isoformat(),
            })

        except Exception as e:
            conn.rollback()
            results.append({"file": filename, "status": "error", "message": str(e)})

    cursor.close()
    conn.close()
    return results
