import os
import re
import pandas as pd
from io import StringIO
from db import get_connection
from unidecode import unidecode

zonesoft_link = 'https://zsbmsv2.zonesoft.org/#!/rpt-tp-valores-dia'

# ----------------------------
# Normalize text for headers
# ----------------------------
def normalize_text(s):
    if not isinstance(s, str):
        s = str(s)
    s = unidecode(s).lower()
    s = re.sub(r"[^\w]", "", s)  # remove everything except letters/numbers
    return s

# ----------------------------
# Clean TPA number like Excel ="0000992577"
# ----------------------------
def clean_tpa_number(s):
    if pd.isna(s):
        return None
    s = str(s).replace('="', '').replace('"', '')
    digits = re.sub(r"\D", "", s)
    return digits if digits else None

# ----------------------------
# Parse Portuguese-formatted / TPA amounts
# ----------------------------
def parse_tpa_amount(s):
    """
    Parse a Portuguese / TPA amount string into a float.
    Handles:
    - 771",15 → 771.15
    - 1.736,10 → 1736.10
    - 1.750 43 → 1750.43
    - 123 45 → 123.45
    """
    if pd.isna(s):
        return None

    s = str(s).strip()

    if s == "":
        return None

    # 1️⃣ Normalize all whitespace
    s = re.sub(r"\s+", " ", s)

    # 2️⃣ Fix comma as decimal separator
    s = s.replace(",", ".")

    # 3️⃣ Handle thousands with dot + decimal: e.g., 1.736.10 → 1736.10
    # If there are multiple dots, last two digits after last dot are decimal
    if s.count(".") > 1:
        parts = s.split(".")
        decimal = parts[-1]
        integer = "".join(parts[:-1])
        s = f"{integer}.{decimal}"

    # 4️⃣ Handle space as decimal separator (123 45 → 123.45)
    if re.match(r"^\d+ \d{1,2}$", s):
        s = s.replace(" ", ".")

    try:
        return float(s)
    except:
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

    df["balance"] = df["balance"].apply(parse_tpa_amount)
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

def preprocess_tpa_csv_lines(lines):
    fixed_lines = []

    for line in lines:
        # 1️⃣ Remove Excel ="0000992577" style quotes
        line = re.sub(r'=""*0*(\d+)""*', r'\1', line)

        # 2️⃣ Replace comma decimal inside quotes: 771",15 → 771.15
        line = re.sub(r'(\d+)",(\d{1,3})', r'\1.\2', line)

        # 3️⃣ Remove extra quotes around numbers
        line = line.replace('"', '')

        # 4️⃣ Normalize whitespace
        line = re.sub(r'\s+', ' ', line).strip()

        fixed_lines.append(line)

    return fixed_lines

# ----------------------------
# Import bank CSVs
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
            if "dc" in df.columns:
                df["dc"] = df["dc"].astype(str).str.strip().str.upper()

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
            df["amount"] = df["amount"].apply(parse_tpa_amount)
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

# ----------------------------
# Import single TPA CSV
# ----------------------------
def import_single_tpa_csv(file_path):
    filename = os.path.basename(file_path)

    def preprocess_lines(lines):
        fixed = []
        for line in lines:
            # 1️⃣ Remove Excel-style TPA numbers: =""0000992577"" → 0000992577
            line = re.sub(r'=""*0*(\d+)""*', r'\1', line)

            # 2️⃣ Replace comma decimal inside quotes: 771",15 → 771.15
            line = re.sub(r'(\d+)",(\d{1,3})', r'\1.\2', line)

            # 3️⃣ Remove remaining quotes
            line = line.replace('"', '')

            # 4️⃣ Normalize whitespace
            line = re.sub(r'\s+', ' ', line).strip()

            fixed.append(line)
        return fixed

    def parse_tpa_amount(s):
        """Parse Portuguese / TPA amounts into float"""
        if pd.isna(s):
            return None
        s = str(s).strip()
        if s == "":
            return None

        # Normalize whitespace
        s = re.sub(r"\s+", " ", s)

        # Comma as decimal
        s = s.replace(",", ".")

        # Handle multiple dots (thousand separators): 1.736.10 → 1736.10
        if s.count(".") > 1:
            parts = s.split(".")
            decimal = parts[-1]
            integer = "".join(parts[:-1])
            s = f"{integer}.{decimal}"

        # Space as decimal separator: 123 45 → 123.45
        if re.match(r"^\d+ \d{1,2}$", s):
            s = s.replace(" ", ".")

        try:
            return float(s)
        except:
            return None

    try:
        # --- Read raw file ---
        with open(file_path, "rb") as f:
            raw = f.read()
        raw = raw.replace(b"\x00", b"")
        text = raw.decode("cp1252", errors="replace")
        lines = text.splitlines()

        # --- Detect header line ---
        header_index = None
        for i, line in enumerate(lines):
            n = normalize_text(line)
            if "data" in n and "montante" in n and "tpa" in n:
                header_index = i
                break
        if header_index is None:
            return {"file": filename, "status": "error", "message": "TPA table header not found"}

        # --- Preprocess CSV lines ---
        fixed_lines = preprocess_lines(lines[header_index:])
        csv_text = "\n".join(fixed_lines)
        df = pd.read_csv(StringIO(csv_text), sep=";", dtype=str, engine="python", on_bad_lines="skip", skip_blank_lines=True)

        # --- Normalize columns ---
        df.columns = [normalize_text(c) for c in df.columns]

        # --- Detect TPA column ---
        tpa_cols = [c for c in df.columns if "tpa" in c]
        if not tpa_cols:
            return {"file": filename, "status": "error", "message": "TPA column not found"}
        tpa_col = tpa_cols[0]

        # --- Column mapping ---
        col_map = {
            tpa_col: "tpa_number",
            "data": "date",
            "montante": "montante",
            "dc": "dc",
            "tsc": "tsc",
            "montliquido": "montante_liquido"
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        # --- Parse fields ---
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df["tpa_number"] = df["tpa_number"].apply(clean_tpa_number)
        df["montante"] = df["montante"].apply(parse_tpa_amount)

        # Default numeric columns to 0
        for col in ["tsc", "montante_liquido"]:
            if col in df.columns:
                df[col] = df[col].apply(parse_tpa_amount).fillna(0)

        # --- Keep valid rows ---
        df = df.dropna(subset=["date", "montante"])
        if df.empty:
            return {"file": filename, "status": "error", "message": "No valid TPA rows found after parsing"}

        # --- Insert into DB ---
        conn = get_connection()
        cursor = conn.cursor()
        inserted = 0

        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO tpa_movements
                (data, tpa_number, montante, dc, tsc, montante_liquido)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    montante = VALUES(montante),
                    dc = VALUES(dc),
                    tsc = VALUES(tsc),
                    montante_liquido = VALUES(montante_liquido)
                """,
                (
                    row["date"].date(),
                    row.get("tpa_number"),
                    row["montante"],
                    row.get("dc"),
                    row.get("tsc"),
                    row.get("montante_liquido")
                )
            )
            inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "file": filename,
            "status": "ok",
            "rows": inserted,
            "message": f"{inserted} TPA rows imported successfully",
            "min_date": df["date"].min().date().isoformat(),
            "max_date": df["date"].max().date().isoformat(),
        }

    except Exception as e:
        return {"file": filename, "status": "error", "message": str(e)}

# ----------------------------
# Import single bank CSV
# ----------------------------
def import_single_bank_csv(file_path):
    folder = os.path.dirname(file_path)
    filename = os.path.basename(file_path)

    results = import_bank_csvs(folder)
    for r in results:
        if r.get("file") == filename:
            return r

    return {"file": filename, "status": "error", "message": "File not processed"}