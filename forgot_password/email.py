import smtplib     #send mail
from email.message import EmailMessage
from rest_framework.response import Response
import ssl

class email_trigger():
    def sendemail(email_id,message):
        print("email called succesfully")
        from_email='rockragul0303@gmail.com'
        email_password="tgincjhzuhvtwbdx"
        to_email=email_id
        subject = "OTP - DATAHUB "

        body = message

        em = EmailMessage()
        em['from'] = from_email
        em['to'] = to_email
        em['subject'] = subject

        em.set_content(body)

        context = ssl.create_default_context()


        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(from_email, email_password)
                smtp.sendmail([from_email], [to_email], em.as_string())

        print("Email sent Successfully")
        
