import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from db import execute_query, get_connection
from datetime import datetime
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
import plotly.graph_objs as go
import plotly.offline as pyo
import os

sns.set_style("whitegrid")

CHARTS_DIR = "dashboard/static/charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

# ------------------ Utility ------------------
def save_and_show(title):
    """Save chart to file and return path"""
    filename = f"{CHARTS_DIR}/{title}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()
    print(f"📊 Chart exported to {filename}")
    return filename

def export_charts_to_excel(image_paths, output_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "Charts"

    row = 1
    for title, path in image_paths.items():
        ws.cell(row=row, column=1, value=title)
        img = XLImage(path)
        img.anchor = f"A{row+1}"
        ws.add_image(img)
        row += 25

    wb.save(output_file)
    print(f"📘 Excel report created: {output_file}")

# ------------------ Daily Reconciliation ------------------
def plot_daily_reconciliation(start_month=None, end_month=None):
    conn = get_connection()
    cursor = conn.cursor()
    data = execute_query("""SELECT date, sales, bank FROM daily_reconciliation""", fetch=True)
    conn.close()

    if not data:
        print("⚠️ No daily reconciliation data")
        return

    df = pd.DataFrame(data)
    df["difference"] = df["sales"] - df["bank"]
    df["date"] = pd.to_datetime(df["date"])

    # Filter by month if requested
    if start_month:
        df = df[df["date"].dt.to_period("M") >= pd.Period(start_month)]
    if end_month:
        df = df[df["date"].dt.to_period("M") <= pd.Period(end_month)]

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df["sales"], label="Sales", marker="o")
    plt.plot(df["date"], df["bank"], label="Bank deposits", marker="o")
    plt.bar(df["date"], df["difference"], alpha=0.3, label="Difference")
    plt.axhline(0)

    title = "Daily Sales vs Bank Reconciliation"
    if start_month or end_month:
        s = start_month if start_month else "Start"
        e = end_month if end_month else "End"
        title += f" ({s} → {e})"
    plt.title(title)
    plt.xticks(rotation=45)
    plt.legend()

    return save_and_show("daily_reconciliation")

# ------------------ Debit Category ------------------
def plot_debit_categories(start_month=None, end_month=None):
    query = """
        SELECT d.category, SUM(b.amount) AS total
        FROM debit_classifications_applied d
        JOIN bank_transactions b ON d.transaction_id = b.id
        GROUP BY d.category
    """
    data = execute_query(query, fetch=True)
    if not data:
        print("⚠️ No debit category data")
        return

    df = pd.DataFrame(data, columns=["category", "total"]).fillna(0)
    df["total"] = df["total"].astype(float)

    plt.figure(figsize=(10, 6))
    df_top = df.sort_values("total", key=abs, ascending=False).head(10)
    bars = plt.bar(df_top["category"], df_top["total"].abs())
    for bar, val in zip(bars, df_top["total"]):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{val:.2f}",
                 ha="center", va="bottom", fontsize=9)

    title = "Top 10 Debit Categories"
    if start_month or end_month:
        s = start_month if start_month else "Start"
        e = end_month if end_month else "End"
        title += f" ({s} → {e})"
    plt.title(title)
    plt.ylabel("Amount")
    plt.xticks(rotation=45, ha="right")

    return save_and_show("debit_categories_top10")

# ------------------ Monthly Debits ------------------
def plot_monthly_debits(start_month=None, end_month=None):
    data = execute_query("""
        SELECT DATE_FORMAT(transaction_date, '%Y-%m') AS month, SUM(amount) AS total
        FROM bank_transactions
        WHERE transaction_type='debit'
        GROUP BY month
        ORDER BY month
    """, fetch=True)

    if not data:
        print("⚠️ No monthly debit data")
        return

    df = pd.DataFrame(data, columns=["month", "total"])
    df["total"] = df["total"].astype(float)
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m")

    if start_month:
        df = df[df["month"].dt.to_period("M") >= pd.Period(start_month)]
    if end_month:
        df = df[df["month"].dt.to_period("M") <= pd.Period(end_month)]

    plt.figure(figsize=(10, 5))
    plt.bar(df["month"].dt.strftime("%Y-%m"), df["total"].abs())
    title = "Monthly Debit Totals"
    if start_month or end_month:
        s = start_month if start_month else "Start"
        e = end_month if end_month else "End"
        title += f" ({s} → {e})"
    plt.title(title)
    plt.ylabel("Amount")
    plt.xticks(rotation=45)

    return save_and_show("monthly_debits")

# ------------------ Stacked Debit Categories ------------------
def plot_stacked_debit_categories(start_month=None, end_month=None):
    data = execute_query("""
        SELECT DATE_FORMAT(b.transaction_date, '%Y-%m') AS month, d.category, SUM(b.amount) AS total
        FROM debit_classifications_applied d
        JOIN bank_transactions b ON d.transaction_id = b.id
        GROUP BY month, d.category
        ORDER BY month
    """, fetch=True)

    if not data:
        print("⚠️ No stacked debit data")
        return

    df = pd.DataFrame(data, columns=["month", "category", "total"])
    df["total"] = df["total"].astype(float)
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m")

    if start_month:
        df = df[df["month"].dt.to_period("M") >= pd.Period(start_month)]
    if end_month:
        df = df[df["month"].dt.to_period("M") <= pd.Period(end_month)]

    pivot = df.pivot_table(index="month", columns="category", values="total", aggfunc="sum", fill_value=0)
    pivot.plot(kind="bar", stacked=True, figsize=(12, 6))
    title = "Debit Categories by Month (Stacked)"
    if start_month or end_month:
        s = start_month if start_month else "Start"
        e = end_month if end_month else "End"
        title += f" ({s} → {e})"
    plt.title(title)
    plt.ylabel("Amount")
    plt.xticks(rotation=45)

    return save_and_show("stacked_debit_categories")

# ------------------ Debit vs Credit Interactive ------------------
def plot_debit_vs_credit_interactive(start_month=None, end_month=None):
    data = execute_query("""
        SELECT DATE_FORMAT(transaction_date, '%Y-%m') AS month, transaction_type, SUM(amount) AS total
        FROM bank_transactions
        GROUP BY month, transaction_type
        ORDER BY month
    """, fetch=True)

    df = pd.DataFrame(data, columns=["month", "type", "total"])
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m")
    df["type"] = df["type"].str.lower().str.strip()
    df["total"] = df["total"].astype(float)

    if start_month:
        df = df[df["month"].dt.to_period("M") >= pd.Period(start_month)]
    if end_month:
        df = df[df["month"].dt.to_period("M") <= pd.Period(end_month)]

    pivot = df.pivot_table(index="month", columns="type", values="total", aggfunc="sum", fill_value=0)
    debit_series = pivot.get("debit", pd.Series(0, index=pivot.index)).abs()
    credit_series = pivot.get("credit", pd.Series(0, index=pivot.index))
    debit_avg = debit_series.rolling(3).mean()
    credit_avg = credit_series.rolling(3).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=pivot.index, y=credit_series, mode='lines+markers', name='Credit', line=dict(color='green')))
    fig.add_trace(go.Scatter(x=pivot.index, y=debit_series, mode='lines+markers', name='Debit', line=dict(color='red')))
    fig.add_trace(go.Scatter(x=pivot.index, y=credit_avg, mode='lines', name='Credit (3M avg)', line=dict(color='green', dash='dash')))
    fig.add_trace(go.Scatter(x=pivot.index, y=debit_avg, mode='lines', name='Debit (3M avg)', line=dict(color='red', dash='dash')))

    title = "Debit vs Credit with 3-Month Rolling Averages"
    if start_month or end_month:
        s = start_month if start_month else "Start"
        e = end_month if end_month else "End"
        title += f" ({s} → {e})"

    fig.update_layout(title=title, xaxis_title='Month', yaxis_title='Amount', template='plotly_white')
    filename = f"{CHARTS_DIR}/debit_vs_credit_rolling.html"
    pyo.plot(fig, filename=filename, auto_open=False)
    print(f"📊 Interactive chart exported to {filename}")
    return filename

# ------------------ Run All ------------------
def run_all_visualizations(start_month=None, end_month=None):
    plot_daily_reconciliation(start_month, end_month)
    plot_debit_categories(start_month, end_month)
    plot_monthly_debits(start_month, end_month)
    plot_stacked_debit_categories(start_month, end_month)
    plot_debit_vs_credit_interactive(start_month, end_month)
