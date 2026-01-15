def get_daily_reconciliation(cursor):
    cursor.execute("""
        SELECT
            s.sale_date,
            CAST(s.amount AS DECIMAL(12,2)) AS sales,
            CAST(IFNULL(b.bank_amount, 0) AS DECIMAL(12,2)) AS bank
        FROM sales s
        LEFT JOIN (
            SELECT
                transaction_date,
                SUM(amount) AS bank_amount
            FROM bank_transactions
            GROUP BY transaction_date
        ) b
        ON s.sale_date = b.transaction_date
        ORDER BY s.sale_date
    """)

    rows = cursor.fetchall()

    return [
        {
            "date": r[0],
            "sales": float(r[1]),
            "bank": float(r[2]),
        }
        for r in rows
    ]
