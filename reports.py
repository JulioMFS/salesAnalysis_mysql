from db import execute_query
from collections import defaultdict
import pandas as pd

def debit_summary_by_category():
    """Return a summary of debits grouped by category."""
    results = execute_query(
        """
        SELECT dca.category, SUM(bt.amount) as total
        FROM debit_classifications_applied dca
        JOIN bank_transactions bt ON dca.transaction_id = bt.id
        GROUP BY dca.category
        """,
        fetch=True
    )
    return results

def monthly_reconciliation():
    """Compare monthly sales vs monthly bank credits."""
    sales = execute_query(
        "SELECT DATE_FORMAT(sale_date,'%Y-%m') as month, SUM(amount) as total_sales FROM sales GROUP BY month",
        fetch=True
    )
    bank = execute_query(
        "SELECT DATE_FORMAT(transaction_date,'%Y-%m') as month, SUM(amount) as total_credits FROM bank_transactions WHERE transaction_type='credit' GROUP BY month",
        fetch=True
    )

    bank_map = {b['month']: b['total_credits'] for b in bank}
    reconciliation = []
    for s in sales:
        month = s['month']
        sales_total = s['total_sales']
        bank_total = bank_map.get(month, 0)
        reconciliation.append({
            'month': month,
            'sales_total': sales_total,
            'bank_total': bank_total,
            'difference': sales_total - bank_total
        })
    return reconciliation

def export_report_to_excel(data, filename):
    """Export a list of dicts to Excel."""
    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    print(f"Report exported to {filename}")
