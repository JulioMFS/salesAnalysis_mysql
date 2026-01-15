from models import create_tables
from db import execute_query

def migrate_initial_data():
    # Optional: sample debit classifications
    rules = [
        ("REPSOL", "Fuel"),
        ("EDP", "Electricity"),
        ("VNC", "Salaries"),
        ("IVA", "VAT"),
        ("IGFSS", "Segurança Social"),
        ("INSTITUTO REGISTOS", "Serviços Notariado"),
        ("COMPRA", "Compras"),
        ("MANUT CONTA", "Man. Conta"),
        ("PAG", "Fornecedores"),
        ("SCALMATICA", "Sistema POS"),
        ("DEB FACTURAS NETCAIXA", "Man. Conta"),
        ("PROSEGUR", "Alarme"),
        ("MEO", "Internet"),
        ("PROSEGUR", "Alarme"),
        ("Multi Imposto", "Impostos"),
        ("TRF SDT", "Fornecedores"),
        ("RENDA", "Renda"),
        ("DISP CARTAO DEBITO", "Man. Conta"),
        ("IMPOSTO", "Impostos"),
        ("PAGAMENTO", "Pagamento"),
    ]
    for pattern, category in rules:
        execute_query(
            """
            INSERT INTO debit_classifications (description_pattern, category)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                category = VALUES(category)
            """,
            (pattern, category)
        )

    print("Initial debit classification rules inserted.")

migrate_initial_data()
