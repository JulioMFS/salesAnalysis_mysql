from db import execute_query
from collections import defaultdict

def reconcile_sales_vs_bank():
    """
    Automatically matches bank credits to sales by date and amount.
    """
    sales = execute_query(
        "SELECT id, sale_date, amount FROM sales", fetch=True
    )

    bank_credits = execute_query(
        "SELECT id, transaction_date, amount FROM bank_transactions WHERE transaction_type='credit'", fetch=True
    )

    sales_map = defaultdict(list)
    for s in sales:
        sales_map[s['sale_date']].append({'id': s['id'], 'amount': float(s['amount'])})

    bank_map = defaultdict(list)
    for b in bank_credits:
        bank_map[b['transaction_date']].append({
            'id': b['id'],
            'amount': float(b['amount'])
        })

    unmatched_sales = []
    unmatched_credits = []
    duplicates = []

    for date, s_list in sales_map.items():
        b_list = bank_map.get(date, [])
        matched_b_ids = set()

        for sale in s_list:
            match_found = False
            for b in b_list:
                if b['id'] in matched_b_ids:
                    continue
                if abs(sale['amount'] - b['amount']) < 0.01:
                    match_found = True
                    matched_b_ids.add(b['id'])
                    break
            if not match_found:
                unmatched_sales.append({
                    'sale_id': sale['id'],
                    'date': date,
                    'amount': sale['amount']
                })

        for b in b_list:
            if b['id'] not in matched_b_ids:
                unmatched_credits.append({
                    'bank_id': b['id'],
                    'date': date,
                    'amount': b['amount']
                })

    seen = set()
    for b in bank_credits:
        key = (b['transaction_date'], float(b['amount']))
        if key in seen:
            duplicates.append({
                'bank_id': b['id'],
                'date': b['transaction_date'],
                'amount': b['amount']
            })
        else:
            seen.add(key)

    return unmatched_sales, unmatched_credits, duplicates


# ---------------- RUN WHEN SCRIPT IS EXECUTED ----------------
if __name__ == "__main__":
    print("Starting daily reconciliation...")

    unmatched_sales, unmatched_credits, duplicates = reconcile_sales_vs_bank()

    print(f"Unmatched sales: {len(unmatched_sales)}")
    print(f"Unmatched bank credits: {len(unmatched_credits)}")
    print(f"Duplicate bank credits: {len(duplicates)}")

    print("Daily reconciliation completed")
