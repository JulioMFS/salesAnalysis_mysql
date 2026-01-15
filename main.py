from models import create_tables
from migrate import migrate_initial_data
from import_excel import import_sales_excels
from import_csv import import_bank_csvs
from classify import classify_debits
from reports import debit_summary_by_category, monthly_reconciliation, export_report_to_excel
from reconciliation import reconcile_sales_vs_bank
from visualize import (
    plot_daily_reconciliation,
    plot_debit_categories,
    run_all_visualizations, plot_monthly_debits, plot_stacked_debit_categories, plot_debit_vs_credit_interactive
)

from visualize import run_all_visualizations

if __name__ == "__main__":
    # Step 1: Create tables
    create_tables()

    # Step 2: Initial debit classification rules
    migrate_initial_data()

    # Step 3: Import sales from Excel folder
    import_sales_excels("data/sales_excels")

    # Step 4: Import bank transactions from CSV folder
    import_bank_csvs("data/bank_csvs")

    # Step 5: Classify debits automatically
    classify_debits()

    # Step 6: Generate debit summary report
    debit_summary = debit_summary_by_category()
    print("Debit summary by category:", debit_summary)
    export_report_to_excel(debit_summary, "reports/debit_summary.xlsx")

    # Step 7: Generate monthly reconciliation report
    reconciliation = monthly_reconciliation()
    print("Monthly reconciliation:", reconciliation)
    export_report_to_excel(reconciliation, "reports/monthly_reconciliation.xlsx")

    # Step 8: Automatic sales vs bank credit reconciliation
    recon_results = reconcile_sales_vs_bank()
    print("Unmatched sales:", recon_results['unmatched_sales'])
    print("Unmatched bank credits:", recon_results['unmatched_credits'])
    print("Duplicate credits:", recon_results['duplicates'])

    export_report_to_excel(recon_results['unmatched_sales'], "reports/unmatched_sales.xlsx")
    export_report_to_excel(recon_results['unmatched_credits'], "reports/unmatched_bank_credits.xlsx")
    export_report_to_excel(recon_results['duplicates'], "reports/duplicate_bank_credits.xlsx")

    # ---- Step 9: Visualization ----
    # ---- Step 9: Visualization ----
    print("Generating charts...")
    daily_file = plot_daily_reconciliation()
    debit_cat_file = plot_debit_categories()
    monthly_file = plot_monthly_debits()
    stacked_file = plot_stacked_debit_categories()
    credit_debit_file = plot_debit_vs_credit_interactive()
    print("Charts generated:")
    print(daily_file, debit_cat_file, monthly_file, stacked_file, credit_debit_file)


