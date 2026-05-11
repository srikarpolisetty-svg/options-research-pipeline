import os
import smtplib
from email.message import EmailMessage

from dotenv import load_dotenv


load_dotenv()


def send_text(message: str):
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    alert_from = os.getenv("ALERT_EMAIL_FROM") or smtp_username
    alert_to = os.getenv("ALERT_EMAIL_TO")

    if not smtp_username or not smtp_password or not alert_from or not alert_to:
        print("[ALERT EMAIL] skipped: SMTP_USERNAME, SMTP_PASSWORD, and ALERT_EMAIL_TO must be set")
        return False

    msg = EmailMessage()
    msg.set_content(message)
    msg["From"] = alert_from
    msg["To"] = alert_to
    msg["Subject"] = "Trading Alert"

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as smtp:
            smtp.login(smtp_username, smtp_password)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print("ERROR in send_text:", repr(e))
        return False
