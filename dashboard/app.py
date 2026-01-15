from flask import Flask, render_template, request
import os
from datetime import datetime, timedelta
from db import execute_query

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static")
)


@app.route("/", methods=["GET"])
def dashboard():

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    daily_reconciliation = []

    if start_date and end_date:

        raw_days = execute_query(
            """
            SELECT
                d.day,
                COALESCE(s.sales_total, 0) AS sales_total,
                COALESCE(b.bank_normal, 0) AS bank_normal,
                COALESCE(b.bank_deposito, 0) AS bank_deposito
            FROM (
                SELECT DISTINCT sale_date AS day
                FROM sales
                WHERE sale_date BETWEEN %s AND %s
            ) d
            LEFT JOIN (
                SELECT sale_date, SUM(amount) AS sales_total
                FROM sales
                GROUP BY sale_date
            ) s ON s.sale_date = d.day
            LEFT JOIN (
                SELECT
                    transaction_date,
                    SUM(CASE WHEN TRIM(description) != 'DEPOSITO' THEN amount ELSE 0 END) AS bank_normal,
                    SUM(CASE WHEN TRIM(description) = 'DEPOSITO' THEN amount ELSE 0 END) AS bank_deposito
                FROM bank_transactions
                WHERE transaction_type = 'credit'
                GROUP BY transaction_date
            ) b ON b.transaction_date = d.day
            ORDER BY d.day
            """,
            (start_date, end_date),
            fetch=True
        )

        credits_since_last_deposito = 0
        accumulated_difference = 0
        last_deposito_day = None

        for row in raw_days:
            day = row["day"]
            sales = row["sales_total"]
            bank_normal = row["bank_normal"]
            deposito = row["bank_deposito"]

            if deposito > 0:
                credits_since_last_deposito = deposito
                last_deposito_day = day - timedelta(days=1)

            credits_since_last_deposito += bank_normal
            difference = sales - credits_since_last_deposito
            accumulated_difference += difference

            daily_reconciliation.append({
                "day": day,
                "sales": sales,
                "credits": credits_since_last_deposito,
                "difference": difference,
                "accumulated_difference": accumulated_difference,
                "from_date": last_deposito_day
            })

    return render_template(
        "dashboard.html",
        daily_reconciliation=daily_reconciliation
    )


# -------------------------------------------------
# Bank credit drill-down
# -------------------------------------------------
@app.route("/bank_credits/<date>")
def bank_credit_detail(date):

    end_date = datetime.strptime(date, "%Y-%m-%d").date()

    # Preserve dashboard filters
    start_filter = request.args.get("start_date")
    end_filter = request.args.get("end_date")

    # Find last DEPOSITO before this date
    last_deposito_row = execute_query(
        """
        SELECT MAX(transaction_date) AS prev_date
        FROM bank_transactions
        WHERE TRIM(description) = 'DEPOSITO'
          AND transaction_date < %s
        """,
        (end_date,),
        fetch=True
    )

    start_date = last_deposito_row[0]["prev_date"] if last_deposito_row else None

    # Fetch contributing bank credits (excluding DEPOSITO)
    bank_rows = execute_query(
        """
        SELECT transaction_date, amount, description, source_file
        FROM bank_transactions
        WHERE transaction_type = 'credit'
          AND TRIM(description) != 'DEPOSITO'
          AND transaction_date > %s
          AND transaction_date <= %s
        ORDER BY transaction_date
        """,
        (start_date or datetime(1900, 1, 1).date(), end_date),
        fetch=True
    )

    deposito_row = execute_query(
        """
        SELECT SUM(amount) AS deposito
        FROM bank_transactions
        WHERE TRIM(description) = 'DEPOSITO'
          AND transaction_date <= %s
        """,
        (end_date,),
        fetch=True
    )
    deposito_amount = deposito_row[0]["deposito"] if deposito_row else 0
    # ✅ THIS IS WHERE IT GOES
    total_credits = sum((row["amount"] or 0) for row in bank_rows)

    return render_template(
        "bank_credit_detail.html",
        credit_date=end_date,
        from_date=start_date,
        bank_rows=bank_rows,
        total_credits=total_credits,
        deposito_amount=deposito_amount,
        start_filter=start_filter,
        end_filter=end_filter
    )


if __name__ == "__main__":
    app.run(debug=True)
