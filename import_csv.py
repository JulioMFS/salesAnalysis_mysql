import os
import re
import unicodedata
import pandas as pd
from io import StringIO
from db import get_connection
zonesoft_link = 'https://zsbmsv2.zonesoft.org/#!/rpt-tp-valores-dia'
# ----------------------------
# Normalize text for header detection
# ----------------------------
def normalize_text(text):
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# ----------------------------
# Parse Portuguese-formatted amounts
# ----------------------------
def parse_pt_amount(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    if value == "":
        return None

    negative = False
    if value.startswith("-"):
        negative = True
        value = value[1:]

    value = value.replace("\t", "").replace(" ", "")
    value = value.replace(".", "").replace(",", ".")

    try:
        amount = float(value)
        return -amount if negative else amount
    except ValueError:
        return None

# ----------------------------
# Detect start of transaction table in CGD or similar CSVs
# ----------------------------
def find_transaction_table_start(file_path):
    with open(file_path, "rb") as f:
        raw_bytes = f.read()
    raw_bytes = raw_bytes.replace(b"\x00", b"").replace(b"\x0c", b"\n")
    text = raw_bytes.decode("cp1252", errors="replace")
    lines = text.splitlines()
    for i, line in enumerate(lines):
        normalized = normalize_text(line)
        if "data" in normalized and "montante" in normalized and (
            "descricao" in normalized or "mov" in normalized
        ):
            return i, lines
    return None, lines

# ----------------------------
# Debit / credit auto-detection
# ----------------------------
def apply_debit_credit_sign(df):
    if "balance" not in df.columns:
        return df

    df["balance"] = df["balance"].apply(parse_pt_amount)
    signed = []
    prev_balance = None

    for _, row in df.iterrows():
        amount = row["amount"]
        balance = row["balance"]

        if pd.isna(amount):
            signed.append(None)
            prev_balance = balance
            continue

        if amount < 0:
            signed.append(amount)
            prev_balance = balance
            continue

        if prev_balance is not None and balance is not None:
            delta = balance - prev_balance
            if abs(delta - amount) < 0.01:
                signed.append(amount)
            elif abs(delta + amount) < 0.01:
                signed.append(-amount)
            else:
                signed.append(amount)
        else:
            signed.append(amount)

        prev_balance = balance

    df["amount"] = signed
    return df

# ----------------------------
# Main import function with results + summary
# ----------------------------
def import_bank_csvs(folder_path):
    results = []
    conn = get_connection()
    cursor = conn.cursor()

    for filename in os.listdir(folder_path):
        if not filename.lower().endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, filename)

        try:
            header_row, lines = find_transaction_table_start(file_path)
            if header_row is None:
                results.append({"file": filename, "status": "error", "message": "No transaction table found"})
                continue

            csv_text = "\n".join(lines[header_row:])
            df = pd.read_csv(StringIO(csv_text), sep=";", header=0, dtype=str, engine="python", on_bad_lines="skip")
            df.columns = df.columns.str.lower().str.strip().str.replace(".", "", regex=False)
            df = df.rename(columns={
                "data mov": "movement_date",
                "data-valor": "value_date",
                "descrição": "description",
                "montante": "amount",
                "saldo contabilístico após movimento": "balance",
            })

            if "movement_date" not in df.columns or "amount" not in df.columns:
                results.append({"file": filename, "status": "error", "message": "Missing required columns (movement_date, amount)"})
                continue

            df["movement_date"] = pd.to_datetime(df["movement_date"], dayfirst=True, errors="coerce")
            df["value_date"] = pd.to_datetime(df["value_date"], dayfirst=True, errors="coerce")
            df["amount"] = df["amount"].apply(parse_pt_amount)
            df["description"] = df.get("description", "").fillna("").astype(str)
            df = df.dropna(subset=["movement_date", "amount"])
            if df.empty:
                results.append({"file": filename, "status": "error", "message": "No valid transactions"})
                continue

            df = apply_debit_credit_sign(df)
            df["transaction_type"] = df["amount"].apply(lambda x: "debit" if x < 0 else "credit")

            inserted = 0
            for _, row in df.iterrows():
                cursor.execute(
                    """
                    INSERT INTO bank_transactions
                        (movement_date, transaction_date, description, amount, transaction_type)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        description = VALUES(description),
                        amount = VALUES(amount),
                        transaction_type = VALUES(transaction_type),
                        transaction_date = VALUES(transaction_date)
                    """,
                    (row["movement_date"].date(),
                     row["value_date"].date() if not pd.isna(row["value_date"]) else None,
                     row["description"][:500],
                     row["amount"],
                     row["transaction_type"])
                )
                inserted += 1

            conn.commit()
            results.append({
                "file": filename,
                "status": "ok",
                "message": f"{inserted} rows imported",
                "rows": inserted,
                "min_date": df["movement_date"].min().date().isoformat(),
                "max_date": df["movement_date"].max().date().isoformat(),
            })

        except Exception as e:
            results.append({"file": filename, "status": "error", "message": str(e)})

    cursor.close()
    conn.close()
    return results

def import_single_bank_csv(file_path):
    folder = os.path.dirname(file_path)
    filename = os.path.basename(file_path)

    results = import_bank_csvs(folder)

    # Return ONLY the result for this file
    for r in results:
        if r.get("file") == filename:
            return r

    return {
        "file": filename,
        "status": "error",
        "message": "File not processed"
    }
