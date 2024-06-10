import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from config import Config

class Email:
    def __init__(self,
                 email_template_path:str,
                 attachments_dir:str):
        self.config = Config()
        self.email_template_path = email_template_path
        self.attachments_dir = attachments_dir
        self.message = self.create_email()

    def create_email(self):
        sender_email = "ibss-central@calacademy.org"
        # copied from webasset importer. not sure if correct

        recipient_list = []
        for email_address in self.config.get_list("mailing_list", "mailing_list"):
            recipient_list.append(email_address)
        recipient_list_as_str = ", ".join(recipient_list)

        # Read the subject and body from the content file
        with open(self.email_template_path, "r") as content_file:
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
        for root, _, files in os.walk(self.attachments_dir):
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

    def send_email(self):
    # copied from webasset importer
        # with smtplib.SMTP('localhost') as server:
        #     server.send_message(message)
        print("Printing the message for now since I can't send from localhost!")
        print(self.message)

def main():
    email = Email('email_template.txt',
                  'generated_csvs2')
    email.create_email()
    email.send_email()

if __name__ == '__main__':
    main()

