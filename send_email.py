from email.mime.image import MIMEImage
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import sqlalchemy as sa
import yaml
from db_connection import DBConnection
from config import Config

config = Config()
FROM_YEAR = config.get_int("years", "from_year")
TO_YEAR = config.get_int("years", "to_year")

def create_engine():
    with open('db_connection.yml', 'r') as file:
        config = yaml.safe_load(file)

    database_url = config['database_url']
    database_port = config['database_port']
    database_password = config['database_password']
    database_user = config['database_user']
    database_name = config['database_name']

    connection_string = f"mysql+pymysql://{database_user}:{database_password}@{database_url}:{database_port}/{database_name}"

    # Return SQL engine
    return sa.create_engine(connection_string)

def casworks_to_df_csv(engine, output_file):
    """
    Generate a csv of CAS works to attach to an email.

    Args:
    engine (sqlalchemy engine instance)
    output_file (str): csv path of query results

    Returns:
    df: (pandas df) df of query results
    """
    query = f"""
        WITH cas_pubs AS (
            SELECT * FROM `publications`.`comprehensive_global_works_v3` 
             WHERE institution_name = 'California Academy of Sciences'
             OR author_orcid in (SELECT author_orcid FROM authors where author_orcid != 'NULL' and author_active=1)
            )
            SELECT 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                GROUP_CONCAT(DISTINCT author_name SEPARATOR ', ') AS authors_concatenated,
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count

            FROM 
                cas_pubs

            GROUP BY 
                work_id,
                work_doi,
                work_display_name,
                work_publication_date,
                work_publication_year,
                work_publisher,
                work_journal,
                work_sustainable_dev_goal,
                work_is_open_access,
                work_cited_by_count

             HAVING
                 work_publication_year = {FROM_YEAR}
            ORDER BY authors_concatenated;
"""
    df = pd.read_sql_query(query, engine)
    df.to_csv(output_file, index=False)
    return df

# this is not a very helpful vis
def create_lineplot(df, time_period=FROM_YEAR):
    plt.figure(figsize=(10, 5))
    grouped_df = df.groupby('work_publication_date').count().reset_index()
    plt.plot(grouped_df['work_publication_date'], grouped_df['work_id'])
    plt.title(f'Works published over {time_period}')
    plt.xlabel('Date')
    plt.ylabel('Publications')
    plt.grid(True)
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Adjust the interval based on your data span
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gcf().autofmt_xdate() 
    plt.savefig(f'email_attachments/publications_{FROM_YEAR}.png')
    plt.close()

def create_email(subject, body, attachment_filename_list):
    sender_email = ""
    receiver_email = ""
    password = ""

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # Attach the body with the email
    message.attach(MIMEText(body, "plain"))

    # Attach the CSV file + visualizations
    for attachment_filename in attachments:
        with open(attachment_filename, "rb") as file:
            if attachment_filename.endswith('.png'):
                part = MIMEImage(file.read(), Name=attachment_filename)
            else:
                part = MIMEApplication(
                    file.read(),
                    Name=attachment_filename
            )
    part['Content-Disposition'] = f'attachment; filename="{attachment_filename}"'
    message.attach(part)

    return message

    # Send the email - need help with this
    # server = smtplib.SMTP('smtp.gmail.com', 587) 
    # server.starttls() # encrypts email
    # server.login(sender_email, password)
    # server.sendmail(sender_email, receiver_email, message.as_string())
    # server.quit()

engine = create_engine()
outfile = f'email_attachments/works_{FROM_YEAR}.csv'
df = casworks_to_df_csv(engine, outfile )
create_lineplot(df)
attachments = [f'email_attachments/publications_{FROM_YEAR}.png',
               f'email_attachments/works_{FROM_YEAR}.csv']
email = create_email("Test Publications",
                     "This is a test",
                     attachments)
print(email)