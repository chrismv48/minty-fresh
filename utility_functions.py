from email.mime.base import MIMEBase
import math


def df_to_json(df, columns=False):
    """
    Returns columns in a Pandas dataframe as a JSON object
    with the following structure:
        {
        "col_name":[val1,val2,val3],
        "col_name2":[val1,val2,val3]
        }
    """
    if columns:
        df_columns = columns
    else:
        df_columns = df.columns

    json_dict = {}
    for col in df_columns:
        json_dict.update({col: list(df[col].values)})

    return json_dict


def format_col(df, col_name, rounding=0, currency=False, percent=False):
    """Function to format numerical Pandas Dataframe columns (one at a time). WARNING: This function will convert the column to strings. Apply this function as the last step in your script.

    Output is the formatted column.

    Parameters:

    df = the dataframe object
    col_name = the name of the column as a string
    rounding = decimal places to round to. ie 0 means round to the nearest whole number
    currency = adds the $ symbol
    percent = adds the % symbol and multiplies by 100
    """

    import locale
    import math

    locale.setlocale(locale.LC_ALL, 'en_US.utf8')
    round_by = '{:,.%sf}' % str(rounding)

    if currency == True:
        return df[col_name].apply(lambda x: '$' + round_by.format(x) if math.isnan(x) == False else x)
    elif percent == True:
        return df[col_name].apply(lambda x: round_by.format(x * 100) + '%' if math.isnan(x) == False else x)
    else:
        return df[col_name].apply(lambda x: round_by.format(x) if math.isnan(x) == False else x)


def format_value(value, rounding=0, currency=False, percent=False):
    import locale

    locale.setlocale(locale.LC_ALL, 'en_US.utf8')
    round_by = '{:,.%sf}' % str(rounding)

    if currency == True:
        return '$' + round_by.format(value)
    elif percent == True:
        return round_by.format(value * 100) + '%'
    else:
        return round_by.format(value)


def format_df(df):
    """Attempts to autoformat numbers based on the column names.
    if %: format as percent
    if imps: format as integer with commas
    if revenue, cost, cpm, rpm, profit: format as currency with 2 decimals
    """
    for col in df.columns:
        if '%' in col:
            df[col] = format_col(
                df, col, rounding=0, currency=False, percent=True)
        if 'imps' in col:
            df[col] = format_col(
                df, col, rounding=0, currency=False, percent=False)
        if any([x in col for x in ['revenue', 'cost', 'cpm', 'rpm', 'profit']]):
            df[col] = format_col(
                df, col, rounding=2, currency=True, percent=False)


def sql_list(iterable, format="int"):
    """Function to convert a Pandas dataframe column/series into an SQL friends string of comma separate values.

    Parameters
    ==========================
    iterable: can be any type of array, list, or pandas column/series
    format: "int" or "string". "int" returns string with no quotes, "string" does.
    """
    sql_list = ''
    if format == "string":
        for i in iterable:
            sql_list += "'" + str(i) + "'" + ","

    elif format == "int":
        for i in iterable:
            sql_list += str(i) + ','

    else:
        print 'Incorrect format parameter. Choose string or int.'

    sql_list = sql_list[:-1]  # delete last comma
    return sql_list


def reorder_df_cols(df, new_order, print_summary=True):
    """
    Function reorders columns in a Pandas DataFrame by using the column order index.

    Parameters
    =======================================
    df: the dataframe for which you'd like to reorder columns
    new_order: a list of integer values that correspond to the positions of the columns in the new order. Example: if columns in a df are 'a' 'b' 'c', and you
    wanted to reorder to be 'b', 'a', 'c', the new_order list would be [1,0,2]
    print_summary: if True, prints the original column order and index values and the new column order and index values
    """

    orig_cols = df.columns.tolist()

    if print_summary:
        print "Original Order:\n"
        for i in enumerate(orig_cols):
            print i

    new_cols = [orig_cols[i] for i in new_order]
    df = df[new_cols]

    if print_summary:
        print "\nNew Order:\n"
        for i in enumerate(new_cols):
            print i
    return df


def send_email(to_addr_list,
               subject,
               body,
               from_addr,
               smtpserver="mail.adnxs.net",
               cc_addr_list=None,
               attachments=None,
               login=None,
               password=None
               ):
    """
    Function to send emails using Python.

    Parameters
    ==================================
    to_addr_list: Supply a list of recipients (even if only sending to one recipient). Example=['carmstrong@appnexus.com','you@appnexus.com']

    subject: The email subject as a string

    body: Body of the email. This can be HTML or plain text.

    from_addr: The name and/or email of the sender. This can be any string value but it is recommend to follow the format:
               Display Name <any_email_prefix@appnexus.com>. For example if you are using this for the Manual Exclusions script,
               you might set the from_addr to be: Manual Exclusions Alert <my-alerts@appnexus.com>

    smtpserver: default is mail.adnxs.net. Do not change unless you know what you're doing.

    cc_addr_list: Provide cc email recipients in the same format as to_addr_list

    attachments: Provide list of attachment locations. If in same directory as script, simply input the filename.
    """
    import smtplib
    import os
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email import Encoders
    from bs4 import BeautifulSoup as bs

    msgRoot = MIMEMultipart('mixed')
    msg = MIMEMultipart('alternative')

    soup = bs(body)
    plain_text_body = str(soup.getText())

    plain = MIMEText(plain_text_body, 'plain')
    html = MIMEText(body, 'html')

    msgRoot.add_header('From', from_addr)
    msgRoot.add_header('To', ','.join(to_addr_list))
    msgRoot.add_header('Subject', subject)

    if attachments != None:
        for attachment in attachments:
            attached_file = MIMEBase('application', "octet-stream")
            attached_file.set_payload(open(attachment, "rb").read())
            Encoders.encode_base64(attached_file)
            attached_file.add_header(
                'Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            msgRoot.attach(attached_file)

    # msg.attach(plain)
    msg.attach(html)
    msgRoot.attach(msg)
    server = smtplib.SMTP(smtpserver)
    server.starttls()
    server.login(login, password)
    server.sendmail(from_addr, to_addr_list, msgRoot.as_string())
    server.quit()


def format_col_names(input_df):
    for col in input_df.columns:
        new_col = col.replace('_', ' ')
        new_col = str.title(new_col)
        # print new_col
        input_df = input_df.rename(columns={col: new_col})
    return input_df


def format_trend_col(x):
    # format_dict[col] = lambda x: '{0:.0%}'.format(x)
    if math.isnan(x) == False:
        if x > .2:
            return "<span class='positive_trend'>" + \
                   '{0:.0%}'.format(x) + \
                   "</span>"
        elif x < -.2:
            return "<span class='negative_trend'>" + \
                   '{0:.0%}'.format(x) + \
                   "</span>"
        else:
            return '{0:.0%}'.format(x)
    else:
        return x
