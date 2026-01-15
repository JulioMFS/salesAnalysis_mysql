import os
import re
import unicodedata
import pandas as pd
from io import StringIO
from db import get_connection


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
    # Remove NULL bytes
    raw_bytes = raw_bytes.replace(b"\x00", b"")
    # Replace page breaks with newline
    raw_bytes = raw_bytes.replace(b"\x0c", b"\n")
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
# Debit / credit auto-detection using balance
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
                signed.append(amount)  # credit
            elif abs(delta + amount) < 0.01:
                signed.append(-amount)  # debit
            else:
                signed.append(amount)
        else:
            signed.append(amount)

        prev_balance = balance

    df["amount"] = signed
    return df


# ----------------------------
# Main import function
# ----------------------------
def import_bank_csvs(folder_path):
    conn = get_connection()
    cursor = conn.cursor()
    total_imported = 0

    for filename in os.listdir(folder_path):
        if not filename.lower().endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, filename)

        try:
            header_row, lines = find_transaction_table_start(file_path)
        except Exception as e:
            print(f"❌ Failed to read {filename}: {e}")
            continue

        if header_row is None:
            print(f"⚠️ No transaction table found in {filename}")
            continue

        # Join all lines from the table start to the end for pandas
        csv_text = "\n".join(lines[header_row:])

        try:
            df = pd.read_csv(
                StringIO(csv_text),
                sep=";",
                header=0,
                dtype=str,
                engine="python",
                on_bad_lines="skip",
            )
        except Exception as e:
            print(f"❌ Failed to parse table in {filename}: {e}")
            continue

        # Normalize column names
        df.columns = df.columns.str.lower().str.strip().str.replace(".", "", regex=False)

        # Rename known Portuguese columns
        df = df.rename(columns={
            "data mov": "date",
            "data-valor": "value_date",
            "descrição": "description",
            "montante": "amount",
            "saldo contabilístico após movimento": "balance",
        })

        if "date" not in df.columns or "amount" not in df.columns:
            print(f"⚠️ Missing required columns in {filename}")
            continue

        # Parse fields
        df["date"] = pd.to_datetime(df["value_date"], dayfirst=True, errors="coerce")
        df["amount"] = df["amount"].apply(parse_pt_amount)
        df["description"] = df.get("description", "").fillna("").astype(str)

        # Drop invalid rows
        df = df.dropna(subset=["date", "amount"])
        if df.empty:
            print(f"⚠️ No valid transactions in {filename}")
            continue

        # Apply debit / credit detection
        df = apply_debit_credit_sign(df)
        df["transaction_type"] = df["amount"].apply(lambda x: "debit" if x < 0 else "credit")
        # Insert into database
        min_date = df["date"].min().date()
        max_date = df["date"].max().date()
        inserted = 0

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO bank_transactions
                    (transaction_date, description, amount, transaction_type)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    description = VALUES(description),
                    amount = VALUES(amount),
                    transaction_type = VALUES(transaction_type)
                """,
                (row["date"].date(), row["description"][:500], row["amount"], row["transaction_type"])
            )
            inserted += 1

        conn.commit()
        total_imported += inserted
        print(f"✔ {filename}: {inserted} rows ({min_date} → {max_date})")

    print(f"\nTotal bank rows imported: {total_imported}")
    cursor.close()
    conn.close()
