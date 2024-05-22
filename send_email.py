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
engine = create_engine()

# FROM_YEAR = config.get_int("years", "from_year")
# TO_YEAR = config.get_int("years", "to_year")

def create_email(subject, body, attachments):
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

# attach from './generated_csvs' ?


