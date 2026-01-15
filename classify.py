from db import execute_query, get_connection

def classify_debits():
    """Classify all unclassified debits safely."""
    # Get all unclassified debits
    debits = execute_query(
        """
        SELECT bt.id, bt.description
        FROM bank_transactions bt
        LEFT JOIN debit_classifications_applied dca
        ON bt.id = dca.transaction_id
        WHERE bt.transaction_type='debit' AND dca.id IS NULL
        """,
        fetch=True
    )

    rules = execute_query("SELECT * FROM debit_classifications", fetch=True)

    if not debits or not rules:
        print("No debits or no classification rules found.")
        return

    conn = get_connection()
    cursor = conn.cursor()
    for debit in debits:
        for rule in rules:
            if rule['description_pattern'].lower() in (debit['description'] or '').lower():
                cursor.execute(
                    "INSERT INTO debit_classifications_applied (transaction_id, category) VALUES (%s,%s)",
                    (debit['id'], rule['category'])
                )
                break
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Classified {len(debits)} debits.")
