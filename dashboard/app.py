import json

from flask import Flask, render_template, request, redirect, url_for, flash
from datetime import datetime, timedelta
from db import execute_query
import pandas as pd
import plotly.express as px
from markupsafe import Markup
import subprocess
import os

app = Flask(__name__)
app.secret_key = "dev-secret"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------- DASHBOARD ----------------
@app.route('/', methods=['GET', 'POST'])
@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if request.method == 'POST':
        action = request.form.get('action')

        scripts = {
            'upload_bank': 'import_csv.py',
            'upload_sales': 'import_excel.py',
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
                flash(f"Reconciliation completed successfully")
                if result.stdout:
                    flash(result.stdout)
            else:
                flash("Reconciliation failed")
                flash(result.stderr)

        return redirect(url_for('dashboard'))

    return render_template('dashboard.html')


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
    today = datetime.today().date()
    default_start = datetime(today.year - 1, 1, 1).date()
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

    # --- Fetch categories & transactions like drilldown ---
    categories = execute_query(
        """
        SELECT 
            COALESCE(c.category,'Unclassified') AS category,
            COUNT(*) AS tx_count,
            SUM(t.amount) AS total_amount
        FROM bank_transactions t
        LEFT JOIN debit_classifications c
            ON t.description LIKE CONCAT('%', c.description_pattern, '%')
        WHERE t.transaction_type='debit'
          AND DATE(t.transaction_date) BETWEEN %s AND %s
        GROUP BY category
        ORDER BY total_amount ASC
        """,
        [start_date, end_date],
        fetch=True
    )

    total_amount = sum(c['total_amount'] for c in categories) if categories else 0
    total_tx = sum(c['tx_count'] for c in categories) if categories else 0
    for c in categories:
        c['percent'] = (c['total_amount'] / total_amount * 100) if total_amount else 0

    # --- Fetch transactions for modal/JS ---
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

    # --- Totals for table header ---
    total_sales = sum([c['total_amount'] for c in categories])  # same as total_amount
    total_expenses = 0  # optional
    total_net = total_sales - total_expenses
    # --- Chart data: Expenses vs Sales (monthly) ---
    chart_rows = execute_query(
        """
        SELECT
            DATE_FORMAT(t.transaction_date, '%Y-%m') AS period,
            SUM(CASE WHEN t.transaction_type = 'debit'  THEN t.amount ELSE 0 END) AS expenses,
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

    # --- Render template ---
    return render_template(
        'expenses_vs_sales.html',
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        view=view,
        categories=categories,
        total_amount=total_amount,
        total_tx=total_tx,
        transactions_json=transactions_json,
        chart_json=chart_json  # 👈 REQUIRED
    )


@app.route('/sales_vs_deposits')
def sales_vs_deposits():
    # Fetch sales
    sales = execute_query("SELECT sale_date AS date, amount FROM sales", fetch=True)
    sales_df = pd.DataFrame(sales)
    if not sales_df.empty:
        sales_df['date'] = pd.to_datetime(sales_df['date'])
        sales_df['amount'] = sales_df['amount'].astype(float)
    else:
        sales_df = pd.DataFrame(columns=['date', 'amount'])

    # Fetch bank credits
    credits = execute_query(
        "SELECT transaction_date AS date, amount FROM bank_transactions WHERE transaction_type='credit'", fetch=True
    )
    deposits_df = pd.DataFrame(credits)
    if not deposits_df.empty:
        deposits_df['date'] = pd.to_datetime(deposits_df['date'])
        deposits_df['amount'] = deposits_df['amount'].astype(float)
    else:
        deposits_df = pd.DataFrame(columns=['date', 'amount'])

    # Date filtering
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if start_date:
        start_date = pd.to_datetime(start_date)
    else:
        start_date = sales_df['date'].min() if not sales_df.empty else pd.Timestamp.today()
    if end_date:
        end_date = pd.to_datetime(end_date)
    else:
        end_date = sales_df['date'].max() if not sales_df.empty else pd.Timestamp.today()

    sales_df = sales_df[(sales_df['date'] >= start_date) & (sales_df['date'] <= end_date)]
    deposits_df = deposits_df[(deposits_df['date'] >= start_date) & (deposits_df['date'] <= end_date)]

    # Group bank credits per date
    daily_credits = deposits_df.groupby('date')['amount'].sum().reset_index()
    daily_credits.rename(columns={'amount': 'credit_amount'}, inplace=True)

    # Merge sales and daily credits
    merged = pd.merge(
        sales_df.groupby('date')['amount'].sum().reset_index(),
        daily_credits,
        on='date',
        how='outer'
    ).fillna(0)
    merged = merged.sort_values('date')

    # Compute difference and accumulated
    merged['difference'] = merged['amount'] - merged['credit_amount']
    merged['accumulated'] = merged['difference'].cumsum()

    # Prepare single "period" for template (all in one for now)
    period = {
        'start_date': start_date.date(),
        'end_date': end_date.date(),
        'total_sales': merged['amount'].sum(),
        'total_credits': merged['credit_amount'].sum(),
        'total_diff': merged['difference'].sum(),
        'rows': []
    }

    for _, row in merged.iterrows():
        period['rows'].append({
            'date': row['date'],
            'sales_amount': float(row['amount']),
            'credit_amount': float(row['credit_amount']),
            'difference': float(row['difference']),
            'accumulated': float(row['accumulated'])
        })

    all_periods = [period]

    return render_template(
        'sales_vs_deposits.html',
        all_periods=all_periods,
        start_date=start_date.date(),
        end_date=end_date.date()
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
    from datetime import datetime
    import json

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
    date = request.args.get('date')
    start_period = request.args.get('start_period')
    end_period = request.args.get('end_period')

    date = pd.to_datetime(date)

    # Fetch bank credits for that date
    transactions = execute_query(
        "SELECT transaction_date, description, amount, transaction_type "
        "FROM bank_transactions "
        "WHERE transaction_type='credit' AND DATE(transaction_date)=%s",
        (date.date(),),
        fetch=True
    )

    # Fetch total sales for the same date
    sales = execute_query(
        "SELECT SUM(amount) as total_sales FROM sales WHERE DATE(sale_date)=%s",
        (date.date(),),
        fetch=True
    )
    total_sales = float(sales[0]['total_sales']) if sales and sales[0]['total_sales'] else 0

    # Add sales_amount to each transaction for diff calculation
    for tx in transactions:
        tx['amount'] = float(tx['amount'])
        tx['sales_amount'] = total_sales

    return render_template(
        'bank_details.html',
        transactions=transactions,
        date=date.date(),
        start_period=start_period,
        end_period=end_period
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
        df_exp['amount'] = df_exp['amount'].astype(float)
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

if __name__ == '__main__':
    app.run(debug=True)
