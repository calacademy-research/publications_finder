import os
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
from queries import create_engine

config = Config()

# FROM_YEAR = config.get_int("years", "from_year")
# TO_YEAR = config.get_int("years", "to_year")

def create_email(email_template_path, attachments_dir):
    sender_email = "ibss-central@calacademy.org"
    # copied from webasset importer. not sure if correct

    recipient_list = []
    for email in config.get_list("mailing_list", "mailing_list"):
        recipient_list.append(email)
    recipient_list_as_str = ", ".join(recipient_list)

    # Read the subject and body from the content file
    with open(email_template_path, "r") as content_file:
        lines = content_file.readlines()
        subject = lines[0].strip()
        body = ''.join(lines[1:]).strip()

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient_list_as_str
    message["Subject"] = subject

    # Attach the body with the email
    message.attach(MIMEText(body, "plain"))

    # Attach CSV files of queries by
    # walking through the generated_csvs directory (which is cleared on each new query run)
    for root, _, files in os.walk(attachments_dir):
        for filename in files:
            filepath = os.path.join(root, filename)
            with open(filepath, "rb") as file:
                part = MIMEApplication(
                    file.read(),
                    Name=filename
                )
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                message.attach(part)

    return message

def send_email(message):
# copied from webasset importer
    # with smtplib.SMTP('localhost') as server:
    #     server.send_message(message)
    print("Printing the message for now since I can't send from localhost!")
    print(message)

def main():
    msg = create_email('email_template.txt', 'generated_csvs2')
    send_email(msg)

if __name__ == '__main__':
    main()

