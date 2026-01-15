from db import execute_query

TABLES = {
    "sales": """
    CREATE TABLE IF NOT EXISTS sales (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sale_date DATE NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        payment_type VARCHAR(50),
        description VARCHAR(255),
        source_file VARCHAR(255),
        UNIQUE KEY unique_sale_date (sale_date)
    )
    """,
    "bank_transactions": """
    CREATE TABLE IF NOT EXISTS bank_transactions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        transaction_date DATE NOT NULL,
        description VARCHAR(255),
        amount DECIMAL(12,2) NOT NULL,
        transaction_type ENUM('credit','debit') NOT NULL,
        source_file VARCHAR(255),
        UNIQUE KEY uniq_tx (transaction_date, amount, description)
    )
    """,
    "debit_classifications": """
    CREATE TABLE IF NOT EXISTS debit_classifications (
        id INT AUTO_INCREMENT PRIMARY KEY,
        description_pattern VARCHAR(255) NOT NULL,
        category VARCHAR(100) NOT NULL
    )
    """,
    "debit_classifications_applied": """
    CREATE TABLE IF NOT EXISTS debit_classifications_applied (
        id INT AUTO_INCREMENT PRIMARY KEY,
        transaction_id INT NOT NULL,
        category VARCHAR(100) NOT NULL,
        FOREIGN KEY (transaction_id) REFERENCES bank_transactions(id) ON DELETE CASCADE
    )
    """
}

def create_tables():
    for name, ddl in TABLES.items():
        execute_query(ddl)
        print(f"Table '{name}' ensured.")
