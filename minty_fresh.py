from __future__ import unicode_literals
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

"""A script that extracts transactions from Mint, builds some tables and emails the results"""

import urllib
import datetime
import json
import mintapi
import pandas as pd
import utility_functions as ut
from premailer import transform

with open('config.json') as login_data:
    login_data = json.load(login_data)

category_mapping = pd.read_excel('Mint Categories Mapping.xlsx')

mint = mintapi.Mint(login_data['mint_username'],
                    login_data['mint_password'])
transactions = mint.get_detailed_transactions(include_investment=True,
                                              skip_duplicates=True,
                                              remove_pending=True,
                                              start_date="01/01/15")


def format_transactions(transactions):
    transactions = ut.format_col_names(transactions)
    transactions = transactions.rename(columns={'Category': 'Sub-Category',
                                                'Merchant': 'Description'})
    transactions['Sub-Category'] = transactions['Sub-Category'].str.title()
    transactions['Sub-Category'] = transactions['Sub-Category'].str.replace('Dvds', 'DVDs')
    transactions['Sub-Category'] = transactions['Sub-Category'].str.replace('Hoa Fees', 'HOA Fees')
    transactions['Sub-Category'] = transactions['Sub-Category'].str.replace('Atm', 'ATM')
    transactions['Sub-Category'] = transactions['Sub-Category'].fillna('Uncategorized')
    full_dataset = transactions.merge(category_mapping, on='Sub-Category', how='left')

    parent_cat_rows = full_dataset[full_dataset['Sub-Category'].isin(category_mapping['Parent Category'])]
    parent_cat_rows.loc[:, ('Parent Category')] = parent_cat_rows.loc[:, ('Sub-Category')]
    full_dataset.update(parent_cat_rows)

    assert not full_dataset[full_dataset['Parent Category'].isnull()]['Sub-Category'].unique()

    full_dataset = full_dataset.set_index(pd.DatetimeIndex(full_dataset['Date'])).drop('Date', axis=1)
    full_dataset.to_csv("mint_transactions.csv")
    return full_dataset


def generate_monthly_by_category(formatted_transactions):
    pivoted = formatted_transactions.pivot_table(index=['Type', 'Parent Category'],
                                                 columns=formatted_transactions.index.map(
                                                     lambda t: t.strftime('%Y-%m')),
                                                 values='Amount', aggfunc='sum', margins=False)

    pivoted = pivoted.reindex(['Income', 'Discretionary', 'Non Discretionary', 'Transfer'], level=0)

    return pivoted.ix[:, -6:]


def generate_net_income_table(formatted_transactions):
    mint = mintapi.Mint(login_data['mint_username'],
                        login_data['mint_password'])
    account_data = mint.get_accounts(True)
    total_account_balance = 0

    for account in account_data:
        if account['accountType'] == 'credit':
            total_account_balance -= account['currentBalance']
        else:
            total_account_balance += account['currentBalance']

    running_net_income = 0
    running_gross_income = 0
    monthly_net_income = {}
    months_in_dataset = set(formatted_transactions.index.map(lambda t: t.strftime('%Y-%m')))
    beginning_balance = total_account_balance - formatted_transactions['Amount'].sum()

    for month in list(sorted(months_in_dataset)):  # needs to be a list because sets can't be sorted
        new_beginning_balance = beginning_balance + running_net_income
        month_df = formatted_transactions[formatted_transactions.index.map(lambda t: t.strftime('%Y-%m') == month)]

        regular_income_month = month_df['Amount'][month_df['Sub-Category'] == 'Paycheck'].sum()
        other_income_month = month_df['Amount'][month_df['Parent Category'] == 'Income'].sum()
        other_income_month -= regular_income_month
        running_gross_income += regular_income_month + other_income_month
        discretionary_expenses_month = month_df['Amount'][month_df['Type'] == 'Discretionary'].sum()
        non_discretionary_expenses_month = month_df['Amount'][month_df['Type'] == 'Non Discretionary'].sum()
        other_expenses_month = month_df['Amount'][month_df['Type'] == 'Transfer'].sum()

        net_income = sum(
            [regular_income_month, other_income_month, discretionary_expenses_month, non_discretionary_expenses_month,
             other_expenses_month])
        ending_balance = new_beginning_balance + net_income
        running_net_income += net_income
        running_pct_saved = running_net_income / running_gross_income * 100.0
        monthly_net_income.update({month: [new_beginning_balance,
                                           regular_income_month,
                                           other_income_month,
                                           discretionary_expenses_month,
                                           non_discretionary_expenses_month,
                                           other_expenses_month,
                                           net_income,
                                           ending_balance,
                                           running_net_income,
                                           running_pct_saved]})

    net_income_table = pd.DataFrame(monthly_net_income,
                                    index=["Beginning Balance", "Regular Income", "Other Income",
                                           "Discretionary Expenses",
                                           "Non Discrectionary Expenses", "Other Expenses", "Net Income",
                                           "Ending Balance", "Running Net Income", "Running % Saved"])

    return net_income_table.ix[:, -9:]


def generate_current_month_expenses(formatted_transactions):
    current_month = formatted_transactions.index.month.max()
    current_month_df = formatted_transactions[formatted_transactions.index.month == current_month]
    current_month_expenses = current_month_df.groupby(['Description', 'Parent Category']).agg(
        {"Amount": "sum", "Sub-Category": "count"})
    current_month_expenses = current_month_expenses.rename(columns={"Sub-Category": "Occurrences"})
    current_month_expenses['AbsAmount'] = current_month_expenses['Amount'].apply(abs)
    current_month_expenses = current_month_expenses.sort("AbsAmount", ascending=False)
    current_month_expenses = current_month_expenses.drop('AbsAmount', axis=1)

    return current_month_expenses


def add_links(monthly_by_category):
    category_index = monthly_by_category.index.get_level_values(1)
    category_base_url = 'https://wwws.mint.com/transaction.event#location:'
    query = '{{"query":"category:{}","typeSort":8}}'
    href_string = '<a class="category_links" href="{}">{}</a>'
    new_index = {}
    for category in category_index:
        query_url = query.format(category)
        category_url = category_base_url + urllib.quote(query_url)
        category_html = href_string.format(category_url, category)
        new_index.update({category: category_html})

    return monthly_by_category.rename(index=new_index)


if __name__ == '__main__':
    formatted_transactions = format_transactions(transactions)
    monthly_by_category = generate_monthly_by_category(formatted_transactions)
    monthly_by_category = add_links(monthly_by_category)
    net_income_table = generate_net_income_table(formatted_transactions)
    current_month_expenses = generate_current_month_expenses(formatted_transactions)
    current_month_expenses['Amount'] = current_month_expenses['Amount'].map('{:,.0f}'.format)

    with open("mint_tables.html", "r") as f:
        css = f.read().replace('\n', '')

    email_body = """<html>{}
    <h3>Monthly by Category</h3>{}
            <h3>Net Income Summary</h3>{}
            <h3>Current Month Expenses</h3></html>{}
    """.format(css, monthly_by_category.fillna(0).applymap('{:,.0f}'.format).to_html(escape=False),
               net_income_table.applymap('{:,.0f}'.format).to_html(),
               current_month_expenses.to_html())

    ut.send_email(to_addr_list=[login_data['chris_gmail']],
                  body=transform(email_body).encode('utf-8'),
                  login=login_data['chris_gmail'],
                  password=login_data['gmail_password'],
                  smtpserver='smtp.gmail.com:587',
                  from_addr="Minty Fresh{}".format(login_data['chris_gmail']),
                  subject="Minty Fresh Weekly - {}".format(datetime.datetime.now().strftime("%m-%d-%y"))
                  )
