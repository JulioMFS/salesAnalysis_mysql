import json
from db import get_connection
from flask import Flask, render_template, request, redirect, url_for, flash, abort, Blueprint, jsonify, current_app
from datetime import date, datetime, timedelta
from db import execute_query
import pandas as pd
import os
from decimal import Decimal
import subprocess
import re
import mysql.connector
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from import_csv import import_single_bank_csv
from import_csv import import_single_tpa_csv
from import_pdf import import_single_sales_pdf


app = Flask(__name__)
# ✅ REQUIRED for sessions + flash
app.secret_key = os.environ.get(
    "FLASK_SECRET_KEY",
    "dev-secret-key-change-me"
)
UPLOAD_DIR = os.path.join(os.getcwd(), 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)


@app.route('/upload/<file_type>', methods=['POST'])
def upload(file_type):
    uploaded_files = request.files.getlist('files[]')
    saved_files = []
    all_results = []

    for file in uploaded_files:
        # --- Validation for sales files ---
        if file_type == "sales":
            filename_lower = file.filename.lower()
            if not (filename_lower.startswith("vendas") and filename_lower.endswith(".pdf")):
                return jsonify({
                    "status": "error",
                    "message": f"Invalid file name: {file.filename}. Expected Vendas*.pdf"
                }), 400

        # --- Save file ---
        dest_path = os.path.join(UPLOAD_DIR, file_type, file.filename)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        file.save(dest_path)
        saved_files.append(dest_path)

    # --- Call importers PER FILE (not per folder) ---
    for file_path in saved_files:
        if file_type == "bank":
            all_results.append(import_single_bank_csv(file_path))
        elif file_type == "sales":
            all_results.append(import_single_sales_pdf(file_path))
        elif file_type == "tpa":
            all_results.append((import_single_tpa_csv(file_path)))

    # --- Build summary ---
    def build_import_summary(results):
        summary = {
            "files_total": len(results),
            "files_ok": 0,
            "files_error": 0,
            "rows_total": 0,
            "min_date": None,
            "max_date": None,
        }
        for r in results:
            if r["status"] == "ok":
                summary["files_ok"] += 1
                summary["rows_total"] += r.get("rows", 0)
                dmin = r.get("min_date")
                dmax = r.get("max_date")
                if dmin:
                    summary["min_date"] = dmin if summary["min_date"] is None else min(summary["min_date"], dmin)
                if dmax:
                    summary["max_date"] = dmax if summary["max_date"] is None else max(summary["max_date"], dmax)
            else:
                summary["files_error"] += 1
        return summary

    summary = build_import_summary(all_results)
    return jsonify({
        "status": "success",
        "summary": summary,
        "results": all_results
    })



# ---------------- DASHBOARD ----------------
@app.route('/', methods=['GET', 'POST'])
@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if request.method == 'POST':
        action = request.form.get('action')

        scripts = {
            'upload_bank': 'import_csv.py',
            'upload_sales': 'import_pdf.py',
            'daily_recon': 'reconciliation.py'
        }

        if action in scripts:
            script_path = os.path.join(BASE_DIR, '..', scripts[action])
            result = subprocess.run(
                ['python', script_path],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                flash(f"{action.replace('_', ' ').title()} completed successfully", 'success')
                if result.stdout:
                    flash(result.stdout, 'success')
            else:
                flash(f"{action.replace('_', ' ').title()} failed", 'danger')
                flash(result.stderr, 'danger')

        return redirect(url_for('dashboard'))

    # ---------------- LOAD DEBIT CLASSIFICATIONS ----------------
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM debit_classifications ORDER BY id")
    classifications = cursor.fetchall()
    cursor.close()
    conn.close()

    return render_template('dashboard.html', classifications=classifications)


@app.route("/expenses", methods=["GET", "POST"])
def expenses():
    now = datetime.now()
    # Default end date = current month/year
    end_year = now.year
    end_month = now.month

    # Default start date = 12 months before
    start_date = now - timedelta(days=365)
    start_year = start_date.year
    start_month = start_date.month

    if request.method == "POST":
        start_month = int(request.form.get("start_month", start_month))
        start_year = int(request.form.get("start_year", start_year))
        end_month = int(request.form.get("end_month", end_month))
        end_year = int(request.form.get("end_year", end_year))

    # Pass to template
    return render_template(
        "expenses.html",
        start_month=start_month,
        start_year=start_year,
        end_month=end_month,
        end_year=end_year,
        current_year=now.year,
        # ... other variables like totals, chart_html, monthly
    )

# ---------------- EXPENSES VS SALES ----------------
@app.route('/expenses_vs_sales', methods=['GET', 'POST'])
def expenses_vs_sales():
    today = date.today()
    default_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    default_end = today

    # --- Form inputs ---
    if request.method == "POST":
        start_str = request.form.get("start_date", default_start.strftime("%Y-%m-%d"))
        end_str = request.form.get("end_date", default_end.strftime("%Y-%m-%d"))
        view = request.form.get("view", "monthly")
    else:
        start_str = default_start.strftime("%Y-%m-%d")
        end_str = default_end.strftime("%Y-%m-%d")
        view = "monthly"

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    # ------------------------------------------------------------------
    # CATEGORIES (EXPENSES ONLY — POSITIVE, CONSISTENT WITH CHART)
    # ------------------------------------------------------------------
    categories = execute_query(
        """
        SELECT 
            COALESCE(c.category,'Unclassified') AS category,
            COUNT(*) AS tx_count,
            SUM(ABS(t.amount)) AS total_amount
        FROM bank_transactions t
        LEFT JOIN debit_classifications c
          ON c.id = (
              SELECT dc.id
              FROM debit_classifications dc
              WHERE t.description LIKE CONCAT('%', dc.description_pattern, '%')
              ORDER BY dc.priority ASC, LENGTH(dc.description_pattern) DESC
              LIMIT 1
          )

        WHERE t.transaction_type = 'debit'
          AND DATE(t.transaction_date) BETWEEN %s AND %s
        GROUP BY category
        ORDER BY total_amount DESC
        """,
        [start_date, end_date],
        fetch=True
    )

    total_expenses = sum(c['total_amount'] for c in categories) if categories else 0
    total_tx = sum(c['tx_count'] for c in categories) if categories else 0

    for c in categories:
        c['percent'] = (c['total_amount'] / total_expenses * 100) if total_expenses else 0

    # ------------------------------------------------------------------
    # TRANSACTIONS (FOR MODAL)
    # ------------------------------------------------------------------
    transactions = execute_query(
        """
        SELECT 
            t.transaction_date AS date,
            t.description,
            ABS(t.amount) AS amount,
            COALESCE(c.category,'Unclassified') AS category
        FROM bank_transactions t
        LEFT JOIN debit_classifications c
          ON c.id = (
              SELECT dc.id
              FROM debit_classifications dc
              WHERE t.description LIKE CONCAT('%', dc.description_pattern, '%')
              ORDER BY dc.priority ASC, LENGTH(dc.description_pattern) DESC
              LIMIT 1
          )

        WHERE t.transaction_type = 'debit'
          AND DATE(t.transaction_date) BETWEEN %s AND %s
        ORDER BY t.transaction_date ASC
        """,
        [start_date, end_date],
        fetch=True
    )

    transactions_json = json.dumps(transactions, default=str)

    # ------------------------------------------------------------------
    # CHART DATA (CANONICAL SOURCE OF TRUTH)
    # ------------------------------------------------------------------
    # --- Chart data: Expenses vs Sales (monthly) ---
    chart_rows = execute_query(
        """
        SELECT
            DATE_FORMAT(t.transaction_date, '%Y-%m') AS period,
            SUM(CASE WHEN t.transaction_type = 'debit'  THEN ABS(t.amount) ELSE 0 END) AS expenses,
            SUM(CASE WHEN t.transaction_type = 'credit' THEN t.amount ELSE 0 END) AS sales
        FROM bank_transactions t
        WHERE DATE(t.transaction_date) BETWEEN %s AND %s
        GROUP BY period
        ORDER BY period
        """,
        [start_date, end_date],
        fetch=True
    )

    for r in chart_rows:
        r['net'] = r['sales'] - r['expenses']

    chart_json = json.dumps(chart_rows, default=str)

    # ------------------------------------------------------------------
    # RENDER
    # ------------------------------------------------------------------
    return render_template(
        'expenses_vs_sales.html',
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        view=view,
        categories=categories,
        total_amount=total_expenses,   # ← matches table + chart
        total_tx=total_tx,
        transactions_json=transactions_json,
        chart_json=chart_json
    )


@app.route('/sales_vs_deposits')
def sales_vs_deposits():
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')

    today = date.today()

    if today.day > 8:
        default_start_date = today.replace(day=1)
    else:
        default_start_date = (today.replace(day=1) - timedelta(days=1)).replace(day=1)

    start_date = (
        datetime.strptime(start_date_param, "%Y-%m-%d").date()
        if start_date_param
        else default_start_date
    )

    end_date = (
        datetime.strptime(end_date_param, "%Y-%m-%d").date()
        if end_date_param
        else today
    )

    # ---------------- SALES ----------------
    sales = execute_query("""
        SELECT DATE(sale_date) AS sale_date,
               payment_method,
               SUM(amount) AS amount
        FROM sales
        WHERE DATE(sale_date) <= %s
        GROUP BY DATE(sale_date), payment_method
    """, (end_date,), fetch=True)

    sales_df = pd.DataFrame(sales, columns=["sale_date", "payment_method", "amount"])
    if sales_df.empty:
        sales_df = pd.DataFrame(columns=["sale_date", "payment_method", "amount"])

    # ---------------- BANK ----------------
    bank = execute_query("""
        SELECT DATE(movement_date) AS transaction_date,
               description,
               amount
        FROM bank_transactions
        WHERE transaction_type = 'credit'
          AND (description LIKE '%POS VENDAS%' OR description LIKE '%DEPOSITO%')
          AND DATE(movement_date) <= %s
    """, (end_date,), fetch=True)

    bank_df = pd.DataFrame(bank, columns=["transaction_date", "description", "amount"])
    if bank_df.empty:
        bank_df = pd.DataFrame(columns=["transaction_date", "description", "amount"])

    # ---------------- FIND PREVIOUS DEPOSITO ----------------
    prev_dep_series = bank_df[
        (bank_df.description.str.contains("DEPOSITO", na=False)) &
        (bank_df.transaction_date < start_date)
    ]["transaction_date"]

    prev_deposit_date = prev_dep_series.max() if not prev_dep_series.empty else date(1900, 1 ,1)

    # ---------------- PRELOAD CASH ACCUMULATOR ----------------
    if prev_deposit_date:
        preload_cash_df = sales_df[
            (sales_df.payment_method == "Dinheiro") &
            (sales_df.sale_date >= prev_deposit_date) &
            (sales_df.sale_date < start_date)
        ]
    else:
        preload_cash_df = sales_df[
            (sales_df.payment_method == "Dinheiro") &
            (sales_df.sale_date < start_date)
        ]

    cash_accumulator = Decimal(preload_cash_df["amount"].sum() or 0)

    # ---------------- DAILY GRID ----------------
    all_dates = pd.date_range(start_date, end_date)
    rows = []

    card_balance = Decimal("0.00")
    cash_balance = Decimal("0.00")
    first_deposit_seen = False

    for d in all_dates:
        d = d.date()

        # ---- SALES ----
        day_sales = sales_df[sales_df.sale_date == d]

        card = Decimal(
            day_sales.loc[
                day_sales.payment_method == "Cartão Débito", "amount"
            ].sum() or 0
        )

        cash = Decimal(
            day_sales.loc[
                day_sales.payment_method == "Dinheiro", "amount"
            ].sum() or 0
        )

        total_sales = card + cash

        # ---- BANK ----
        day_bank = bank_df[bank_df.transaction_date == d]

        pos = Decimal(
            day_bank.loc[
                day_bank.description.str.contains("POS VENDAS", na=False), "amount"
            ].sum() or 0
        )

        deposit = Decimal(
            day_bank.loc[
                day_bank.description.str.contains("DEPOSITO", na=False), "amount"
            ].sum() or 0
        )

        # ---- CARD ----
        card_diff = pos - card
        card_balance += card_diff

        # ---- CASH (MATCHES deposit_breakdown EXACTLY) ----
        cash_diff = None
        cash_before_deposit = None
        deposit_note = None

        if deposit > 0:
            # Deposit closes cash cycle up to YESTERDAY
            cash_before_deposit = cash_accumulator
            cash_diff = deposit - cash_accumulator
            cash_balance += cash_diff

            # Reset cycle
            cash_accumulator = Decimal("0.00")

            if not first_deposit_seen:
                deposit_note = (
                    f"Inclui dinheiro desde depósito anterior ({prev_deposit_date})"
                    if prev_deposit_date
                    else "Inclui dinheiro desde início dos registos"
                )
                first_deposit_seen = True

        # Today's cash ALWAYS belongs to next cycle
        cash_accumulator += cash

        if not first_deposit_seen:
                deposit_note = (
                    f"Inclui dinheiro desde depósito anterior ({prev_deposit_date})"
                    if prev_deposit_date
                    else "Inclui dinheiro desde início dos registos"
                )
                first_deposit_seen = True

        total_diff = card_diff + (cash_diff if cash_diff is not None else Decimal("0.00"))

        rows.append({
            "date": d,
            "day_name": d.strftime("%a"),
            "sales_card": card,
            "sales_cash": cash,
            "sales_total": total_sales,
            "bank_pos": pos,
            "bank_deposit": deposit,
            "diff_card": card_diff,
            "diff_cash": cash_diff,
            "diff_total": total_diff,
            "card_balance": card_balance,
            "cash_balance": cash_balance,
            "cash_before_deposit": cash_before_deposit,
            "deposit_note": deposit_note,
        })

    # ---------------- TOTALS ----------------
    total_sales_sum = sum(r["sales_total"] for r in rows)
    total_deposits_sum = sum(r["bank_deposit"] for r in rows)
    total_diff_sum = sum(r["diff_total"] for r in rows)

    all_periods = [{
        "start_date": start_date,
        "end_date": end_date,
        "total_sales": total_sales_sum,
        "total_credits": total_deposits_sum,
        "total_diff": total_diff_sum,
        "rows": rows
    }]

    return render_template(
        "sales_vs_deposits.html",
        start_date=start_date,
        end_date=end_date,
        all_periods=all_periods
    )

# --- Helper function to build period dictionary ---
def build_period(sales_rows, deposits_rows, start_date, end_date):
    # Merge sales and deposits per date
    df = pd.DataFrame({'date': pd.date_range(start_date, end_date)})
    if not sales_rows.empty:
        df = df.merge(sales_rows, on='date', how='left')
    else:
        df['sales_amount'] = 0
    if not deposits_rows.empty:
        df = df.merge(deposits_rows[['date', 'credit_amount']], on='date', how='left')
    else:
        df['credit_amount'] = 0

    df['sales_amount'] = df['sales_amount'].fillna(0)
    df['credit_amount'] = df['credit_amount'].fillna(0)
    df['difference'] = df['credit_amount'] - df['sales_amount']
    df['accumulated'] = df['difference'].cumsum()

    return {
        'start_date': start_date.date(),
        'end_date': end_date.date(),
        'total_sales': df['sales_amount'].sum(),
        'total_credits': df['credit_amount'].sum(),
        'total_diff': df['difference'].sum(),
        'rows': df.to_dict(orient='records')
    }

# ---------------- EXPENSE DRILLDOWN ----------------
@app.route('/expenses_drilldown')
def expenses_drilldown():

    # Get parameters from URL
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    period = request.args.get('period', 'Period')
    view = request.args.get('view', 'monthly')

    # Parse exact period
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    title = period  # e.g., "Nov 2025"

    # Aggregate categories, descending
    categories = execute_query(
        """
        SELECT 
            COALESCE(c.category, 'Unclassified') AS category,
            COUNT(*) AS tx_count,
            SUM(t.amount) AS total_amount
        FROM bank_transactions t
        LEFT JOIN debit_classifications c
            ON t.description LIKE CONCAT('%', c.description_pattern, '%')
        WHERE t.transaction_type='debit'
          AND DATE(t.transaction_date) BETWEEN %s AND %s
        GROUP BY category
        ORDER BY total_amount DESC
        """,
        [start_date, end_date],
        fetch=True
    )

    # Compute totals
    total_amount = sum(c['total_amount'] for c in categories) if categories else 0
    total_tx = sum(c['tx_count'] for c in categories) if categories else 0

    # Compute percentages
    for c in categories:
        c['percent'] = (c['total_amount']/total_amount*100) if total_amount else 0

        # Add period info for drill-down links (optional)
        c['period'] = period
        c['start_date'] = start_date
        c['end_date'] = end_date

    # Fetch all transactions for modal
    transactions = execute_query(
        """
        SELECT 
            t.transaction_date AS date,
            t.description,
            t.amount,
            COALESCE(c.category,'Unclassified') AS category
        FROM bank_transactions t
        LEFT JOIN debit_classifications c
            ON t.description LIKE CONCAT('%', c.description_pattern, '%')
        WHERE t.transaction_type='debit'
          AND DATE(t.transaction_date) BETWEEN %s AND %s
        ORDER BY t.transaction_date ASC
        """,
        [start_date, end_date],
        fetch=True
    )

    transactions_json = json.dumps(transactions, default=str)

    return render_template(
        'expenses_vs_sales_drilldown.html',
        title=title,
        categories=categories,
        total_amount=total_amount,
        total_tx=total_tx,
        transactions_json=transactions_json,
        view=view
    )

@app.route('/bank_details')
def bank_details():

    date_param = request.args.get('date')
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')

    selected_date = (
        pd.to_datetime(date_param).date()
        if date_param else pd.Timestamp.today().date()
    )

    start_date = (
        pd.to_datetime(start_date_param).date()
        if start_date_param else None
    )

    end_date = (
        pd.to_datetime(end_date_param).date()
        if end_date_param else None
    )

    deposits = execute_query(
        "SELECT transaction_date, description, amount "
        "FROM bank_transactions "
        "WHERE transaction_type='credit' "
        "ORDER BY transaction_date",
        fetch=True
    )

    deposits_df = pd.DataFrame(
        deposits,
        columns=['transaction_date', 'description', 'amount']
    )

    if not deposits_df.empty:
        deposits_df['transaction_date'] = pd.to_datetime(
            deposits_df['transaction_date']
        ).dt.date
        deposits_df['amount'] = deposits_df['amount'].astype(float)

    deposito_dates = deposits_df[
        deposits_df['description'] == 'DEPOSITO'
    ]['transaction_date']

    prev_deposito_date = (
        deposito_dates[deposito_dates < selected_date].max()
        if not deposito_dates.empty else selected_date
    )

    filtered_df = deposits_df[
        (deposits_df['transaction_date'] >= prev_deposito_date) &
        (deposits_df['transaction_date'] <= selected_date)
    ].copy()

    filtered_df = filtered_df[
        filtered_df['description'] != 'DEPOSITO'
    ].sort_values('transaction_date')

    filtered_df['credited_date'] = prev_deposito_date

    transactions = filtered_df.to_dict(orient='records')
    total_credits = float(filtered_df['amount'].sum()) if not filtered_df.empty else 0.0

    total_sales = 0.0
    total_diff = total_sales - total_credits

    return render_template(
        'bank_details.html',
        transactions=transactions,
        date=selected_date,
        total_sales=total_sales,
        total_credits=total_credits,
        total_diff=total_diff,
        start_date=start_date,
        end_date=end_date
    )

@app.route('/deposit_breakdown')
def deposit_breakdown():

    deposit_date_param = request.args.get('deposit_date')
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')

    if not deposit_date_param:
        abort(400, "Missing deposit_date")

    deposit_date = pd.to_datetime(deposit_date_param).date()

    start_date = pd.to_datetime(start_date_param).date() if start_date_param else date(1900, 1, 1)
    end_date = pd.to_datetime(end_date_param).date() if end_date_param else None
    start_date1 = start_date - timedelta(days=8)
    # Fetch bank transactions
    deposits = execute_query(
        "SELECT transaction_date, description, amount "
        "FROM bank_transactions "
        "WHERE transaction_type='credit' "
        " AND DATE(transaction_date) BETWEEN %s AND %s"
        "ORDER BY transaction_date",
        [start_date1, end_date],
        fetch=True
    )

    df = pd.DataFrame(
        deposits,
        columns=['transaction_date', 'description', 'amount']
    )

    df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
    df['amount'] = df['amount'].astype(float)

    # Get current depósito
    # Get the depósito that closes this period
    deposito_row = df[
        (df['description'] == 'DEPOSITO ') &
        (df['transaction_date'] >= deposit_date)
        ].sort_values('transaction_date').head(1)

    if deposito_row.empty:
        abort(404, f"No DEPOSITO found on or after {deposit_date}")

    deposito_date = deposito_row['transaction_date'].iloc[0]
    deposito_amount = float(deposito_row['amount'].iloc[0])

     # Find previous depósito
    previous_deposito_date = df[
        (df['description'] == 'DEPOSITO ') &
        (df['transaction_date'] < deposit_date)
    ]['transaction_date'].max()

    # Fetch cash sales (Dinheiro)
    cash_sales = execute_query(
        "SELECT sale_date, amount "
        "FROM sales "
        "WHERE payment_method = 'Dinheiro'",
        fetch=True
    )

    cash_df = pd.DataFrame(cash_sales, columns=['sale_date', 'amount'])
    cash_df['sale_date'] = pd.to_datetime(cash_df['sale_date']).dt.date
    cash_df['amount'] = cash_df['amount'].astype(float)

    # Filter cash used for comparison
    cash_used = cash_df[
        (cash_df['sale_date'] >= previous_deposito_date) &
        (cash_df['sale_date'] < deposit_date)
        ].sort_values('sale_date')

    total_cash = float(cash_used['amount'].sum())
    diff = deposito_amount - total_cash
    if diff is None:
        diff_class = ""
    elif diff < 0:
        diff_class = "diff-negative"
    else:
        diff_class = "diff-positive"

    return render_template(
        'deposit_breakdown.html',
        deposit_date=deposit_date,
        deposito_amount=deposito_amount,
        previous_deposito_date=previous_deposito_date,
        cash_rows=cash_used.to_dict(orient='records'),
        total_cash=total_cash,
        diff=diff,
        diff_class=diff_class,
        start_date=start_date,
        end_date=end_date
    )

@app.route('/expenses_vs_sales_data', methods=['POST'])
def expenses_vs_sales_data():
    """Return JSON with categories, totals, and chart data for given date range + view."""
    data = request.form
    start_str = data.get("start_date")
    end_str = data.get("end_date")
    view = data.get("view", "monthly")

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    # --- Fetch transactions ---
    transactions = execute_query(
        """
        SELECT t.transaction_date AS date,
               t.description,
               t.amount,
               COALESCE(c.category,'Unclassified') AS category
        FROM bank_transactions t
        LEFT JOIN debit_classifications c
            ON t.description LIKE CONCAT('%', c.description_pattern, '%')
        WHERE t.transaction_type='debit'
          AND DATE(t.transaction_date) BETWEEN %s AND %s
        ORDER BY t.transaction_date ASC
        """,
        [start_date, end_date],
        fetch=True
    )

    # Aggregate categories
    df_cat = pd.DataFrame(transactions)
    if not df_cat.empty:
        categories_agg = df_cat.groupby('category').agg(
            tx_count=('amount','count'),
            total_amount=('amount','sum')
        ).reset_index().sort_values('total_amount', ascending=False).to_dict(orient='records')
        total_amount = sum(c['total_amount'] for c in categories_agg)
        total_tx = sum(c['tx_count'] for c in categories_agg)
        for c in categories_agg:
            c['percent'] = (c['total_amount']/total_amount*100) if total_amount else 0
    else:
        categories_agg = []
        total_amount = 0
        total_tx = 0

    # --- Fetch sales for same period ---
    sales = execute_query(
        "SELECT sale_date AS date, amount FROM sales WHERE DATE(sale_date) BETWEEN %s AND %s",
        [start_date, end_date],
        fetch=True
    )

    # --- Prepare Plotly chart data ---
    df_exp = pd.DataFrame(transactions)
    df_sales = pd.DataFrame(sales)

    if not df_exp.empty:
        df_exp['date'] = pd.to_datetime(df_exp['date'])
        df_exp['amount'] = df_exp['amount'].astype(float).abs()
    else:
        df_exp = pd.DataFrame(columns=['date','amount'])

    if not df_sales.empty:
        df_sales['date'] = pd.to_datetime(df_sales['date'])
        df_sales['amount'] = df_sales['amount'].astype(float)
    else:
        df_sales = pd.DataFrame(columns=['date','amount'])

    # Group by view
    if view == "monthly":
        df_exp['period'] = df_exp['date'].dt.to_period('M').dt.to_timestamp()
        df_sales['period'] = df_sales['date'].dt.to_period('M').dt.to_timestamp()
    elif view == "weekly":
        df_exp['period'] = df_exp['date'].dt.to_period('W').apply(lambda r: r.start_time)
        df_sales['period'] = df_sales['date'].dt.to_period('W').apply(lambda r: r.start_time)
    else:
        df_exp['period'] = df_exp['date']
        df_sales['period'] = df_sales['date']

    exp_chart = df_exp.groupby('period')['amount'].sum().reset_index().rename(columns={'amount':'expenses'})
    sales_chart = df_sales.groupby('period')['amount'].sum().reset_index().rename(columns={'amount':'sales'})
    merged_chart = pd.merge(exp_chart, sales_chart, left_on='period', right_on='period', how='outer').fillna(0)
    merged_chart['net'] = merged_chart['sales'] - merged_chart['expenses']

    chart_json = merged_chart.to_dict(orient='records')

    return {
        "categories": categories_agg,
        "total_amount": total_amount,
        "total_tx": total_tx,
        "chart_json": chart_json,
        "transactions_json": transactions  # optional, for modal
    }

# ----- View page -----
@app.route('/debit_classifications', methods=['GET'])
def debit_classifications():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM debit_classifications ORDER BY id")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('debit_classifications.html', classifications=rows)

@app.route('/category_evolution', methods=['GET'])
def category_evolution():
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    first_of_last_month = last_month_end.replace(day=1)

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT DISTINCT category
        FROM debit_classifications
        WHERE category IS NOT NULL
        ORDER BY category
    """)
    categories = [r['category'] for r in cursor.fetchall()]

    cursor.close()
    conn.close()

    return render_template(
            "category_evolution.html",
            start_date=first_of_last_month.isoformat(),
            end_date=today.isoformat(),
            categories=categories
        )

@app.route('/category_evolution_data', methods=['POST'])
def category_evolution_data():
    data = request.form

    start_date_str = data.get("start_date")
    end_date_str = data.get("end_date")

    if not start_date_str or not end_date_str:
        return jsonify({"series": []})

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    view = data.get('view', 'monthly')
    categories = data.getlist('categories[]')

    if not categories:
        return jsonify({"series": []})

    # --- Time grouping ---
    if view == "monthly":
        period_sql = "DATE_FORMAT(t.transaction_date, '%Y-%m-01')"
    elif view == "weekly":
        period_sql = "DATE_SUB(DATE(t.transaction_date), INTERVAL WEEKDAY(t.transaction_date) DAY)"
    else:
        period_sql = "DATE(t.transaction_date)"

    placeholders = ",".join(["%s"] * len(categories))

    rows = execute_query(
        f"""
        SELECT
            {period_sql} AS period,
            c.category,
            SUM(ABS(t.amount)) AS total
        FROM bank_transactions t
        JOIN debit_classifications c
          ON t.description LIKE CONCAT('%', c.description_pattern, '%')
        WHERE t.transaction_type = 'debit'
          AND c.category IN ({placeholders})
          AND DATE(t.transaction_date) BETWEEN %s AND %s
        GROUP BY period, c.category
        ORDER BY period
        """,
        categories + [start_date, end_date],
        fetch=True
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return jsonify({"series": []})

    df['period'] = pd.to_datetime(df['period'])

    # --- Build Plotly series ---
    series = []
    for cat in categories:
        d = df[df.category == cat]
        series.append({
            "name": cat,
            "x": d['period'].dt.strftime('%Y-%m-%d').tolist(),
            "y": d['total'].tolist()
        })

    return jsonify({"series": series})

# ----- Add new -----

# ---------------- Add classification ----------------
@app.route('/add_classification', methods=['POST'])
def add_classification():
    description = request.form.get('description_pattern')
    category = request.form.get('category')
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO debit_classifications (description_pattern, category) VALUES (%s, %s)",
        (description, category)
    )
    conn.commit()
    new_id = cursor.lastrowid
    cursor.close()
    conn.close()
    return jsonify({
        'id': new_id,
        'description_pattern': description,
        'category': category
    })

# ---------------- Edit classification ----------------
@app.route('/edit_classification/<int:id>', methods=['POST'])
def edit_classification(id):
    description = request.form.get('description_pattern')
    category = request.form.get('category')
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE debit_classifications SET description_pattern=%s, category=%s WHERE id=%s",
        (description, category, id)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({
        'id': id,
        'description_pattern': description,
        'category': category
    })

# ---------------- Delete classification ----------------
@app.route('/delete_classification/<int:id>', methods=['POST'])
def delete_classification(id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM debit_classifications WHERE id=%s", (id,))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'id': id})

tpa_bp = Blueprint("tpa", __name__)

def fix_number(value):
    # "1.245\t52" -> 1245.52
    value = value.replace(".", "")
    value = value.replace("\t", ".")
    return float(value)

def fix_tpa(tpa):
    return re.sub(r"[^0-9]", "", tpa)

def fix_date(date_str):
    # 01-10-2025 -> 2025-10-01
    return datetime.strptime(date_str, "%d-%m-%Y").date()

@tpa_bp.route("/upload/tpa", methods=["POST"])
def upload_tpa():
    if "files[]" not in request.files:
        return jsonify({
            "status": "error",
            "message": "No files uploaded"
        }), 400

    files = request.files.getlist("files[]")

    files_ok = 0
    files_error = 0
    rows_total = 0
    results = []
    min_date = None
    max_date = None

    db = mysql.connector.connect(
        host=current_app.config["DB_HOST"],
        user=current_app.config["DB_USER"],
        password=current_app.config["DB_PASSWORD"],
        database=current_app.config["DB_NAME"]
    )
    cur = db.cursor()

    for file in files:
        rows = 0
        try:
            content = file.stream.read().decode("utf-8").splitlines()

            for line in content:
                if not line[:2].isdigit():
                    continue

                parts = line.split(";")

                data = fix_date(parts[0])
                tpa = fix_tpa(parts[1])
                montante = fix_number(parts[4])
                dc = parts[5].strip()
                tsc = fix_number(parts[6])
                mont_liq = fix_number(parts[7])

                cur.execute("""
                    INSERT INTO tpa_movements
                    (data, tpa_number, montante, dc, tsc, montante_liquido)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (data, tpa, montante, dc, tsc, mont_liq))

                rows += 1
                rows_total += 1

                min_date = data if not min_date or data < min_date else min_date
                max_date = data if not max_date or data > max_date else max_date

            files_ok += 1
            results.append({
                "file": file.filename,
                "status": "ok",
                "rows": rows
            })

        except Exception as e:
            files_error += 1
            results.append({
                "file": file.filename,
                "status": "error",
                "error": str(e)
            })

    db.commit()
    cur.close()
    db.close()

    return jsonify({
        "status": "success",
        "summary": {
            "files_total": len(files),
            "files_ok": files_ok,
            "files_error": files_error,
            "rows_total": rows_total,
            "min_date": min_date.isoformat() if min_date else None,
            "max_date": max_date.isoformat() if max_date else None
        },
        "results": results
    })


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001, debug=True)
