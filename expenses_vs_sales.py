from flask import Flask, render_template, request, flash
from markupsafe import Markup
import plotly.express as px
import pandas as pd
from datetime import datetime
from db import execute_query
from utils import get_sales_and_expenses  # function above

@app.route('/expenses_vs_sales', methods=['GET', 'POST'])
def expenses_vs_sales():
    period = request.form.get('period', 'last_3_months')
    today = pd.Timestamp.today()
    start_date = None

    if period == 'last_3_months':
        start_date = today - pd.DateOffset(months=3)
    elif period == 'last_6_months':
        start_date = today - pd.DateOffset(months=6)
    elif period == 'last_year':
        start_date = today - pd.DateOffset(years=1)
    elif period == 'custom' and request.form.get('start_date'):
        start_date = pd.Timestamp(request.form.get('start_date'))

    df_combined = get_sales_and_expenses(start_date)
    if df_combined.empty:
        flash('No data available for the selected period', 'error')
        return render_template('expenses_vs_sales.html', chart_html="")

    # Create grouped bar chart
    fig = px.bar(df_combined, x='date', y=['expenses','sales'], barmode='group', title='Expenses vs Sales')

    # Make expenses bars clickable (drill-down by date)
    fig.update_traces(customdata=df_combined['date'], hovertemplate='Date: %{x}<br>Amount: %{y}<extra></extra>')

    chart_html = Markup(fig.to_html(full_html=False, include_plotlyjs=True))

    # JS to handle click events
    chart_html += Markup("""
    <script>
        const myPlot = document.querySelectorAll('[id^="plotly-"]')[0];
        if (myPlot) {
            myPlot.on('plotly_click', function(data){
                var date = data.points[0].customdata;
                var trace_name = data.points[0].fullData.name;
                if(trace_name === 'expenses'){
                    window.location.href = '/expenses_drilldown/' + encodeURIComponent(date);
                } else if(trace_name === 'sales'){
                    window.location.href = '/sales_drilldown/' + encodeURIComponent(date);
                }
            });
        }
    </script>
    """)

    return render_template('expenses_vs_sales.html', chart_html=chart_html)
