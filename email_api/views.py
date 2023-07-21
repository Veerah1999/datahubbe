from email.message import EmailMessage
# from app2 import password
import ssl
import smtplib
from datahub_v3_app.models import *
from django.http import HttpResponse
from email_api.serializers import *
from rest_framework.views import APIView

def get(request,s):
    print(s)
    data=tenant_user.objects.filter(id=1).values()
    # import pdb
    # pdb.set_trace
    print(data)
    test={}
    for i in data:
        test.update(i)
    print(test)
    mail=test['email']
    id="vishwasanjeev25@gmail.com"
    cc="kowsivji17@gmail.com"

    email_sender = "rockragul0303@gmail.com"
    email_password = "tgincjhzuhvtwbdx"

    email_reciever = mail

    email_cc = cc
    email_bcc = cc

    subject = "your user name and password from datahub"#"Testing for the email Notifications "

    body = f"""hii  from Datahub
    This is your username
    username:  {test['email']} 
    password:  {test['password']}
    Happy Migration
    
    


    Thankyou
    Datahub
    """

    em = EmailMessage()
    em['from'] = email_sender
    em['to'] = email_reciever
    em['cc'] = email_cc
    em['bcc'] = email_bcc
    em['subject'] = subject

    em.set_content(body)

    context = ssl.create_default_context()


    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email_sender, email_password)
            smtp.sendmail([email_sender], [email_reciever, email_cc, email_bcc], em.as_string())

    print("Email sent Successfully")
    return HttpResponse("iam done")