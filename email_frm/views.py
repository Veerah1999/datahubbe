from email.message import EmailMessage
# from app2 import password
import ssl
import smtplib


def email_send(emails,sub,bod):
    email_sender = "rockragul0303@gmail.com"
    email_password = "tgincjhzuhvtwbdx"

    email_reciever = emails

    email_cc = 'sugakldm@gmail.com'
    # email_bcc = str(input("bcc?:"))

    subject = sub

    body = bod

    em = EmailMessage()
    em['from'] = email_sender
    em['to'] = email_reciever
    em['cc'] = email_cc
    # em['bcc'] = email_bcc
    em['subject'] = subject

    em.set_content(body)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.sendmail([email_sender], [email_reciever, email_cc], em.as_string())

    print("Email sent Successfully")

