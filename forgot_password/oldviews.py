from email.message import EmailMessage
# from app2 import password
import ssl
import smtplib

from django.views.decorators.csrf import csrf_exempt
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from rest_framework.views import APIView
from rest_framework.views import Response
from datahub_v3_app.models import tenant_user

import random
from datahub_v3_app.models import forget

class fpassword(APIView):
    def post(self, request):
        data = request.data
        mails = tenant_user.objects.filter(email=data['email']).values()

        print(mails)
        mail_temp = {}
        for i in mails:
            mail_temp.update(i)
        mail = mail_temp['email']
        # mail=mails['email']
        forget_password(request,mail)
        # reset_password(request,mail)
        return Response(mail)


@csrf_exempt
def forget_password(request, mail):

    try:
        user = tenant_user.objects.get(email=mail)
    except ObjectDoesNotExist:
        return render(request, 'error.html', {'message': 'User not found'})
    otp = str(random.randint(100000, 999999))
    forge = forget(otp=otp)# Create an instance of the model

    forge.save()
    email_sender = "rockragul0303@gmail.com"
    email_password ="tgincjhzuhvtwbdx"

    email_reciever = mail

    email_cc = "sugakldm@gmail.com"
    email_bcc = "None"

    subject = "reset password"
    body = f"The otp for Reset Password is {otp}"

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

    response = "Email sent Successfully"

    return  (response)





from django.core.exceptions import ObjectDoesNotExist
from rest_framework.views import APIView
from rest_framework.response import Response
from django.shortcuts import render
# from .models import forget, tenant_user

class verify(APIView):
    def post(self, request):
        email = request.data.get('email')
        otp = request.data.get('otp')

        try:
            user = tenant_user.objects.get(email=email)
        except ObjectDoesNotExist:
            return Response({'message': 'User not found'})

        forget_instance = forget.objects.first()
        if forget_instance != forget_instance.verify_otp(otp):
            forget_instance.delete()
            return Response({'user_id': user.id})
        else:
            return Response({'message': 'Invalid OTP'})


class reset(APIView):
    def post(self, request):
        email = request.data.get('email')
        password = request.data.get('password')

        try:
            user = tenant_user.objects.get(email=email)
        except tenant_user.DoesNotExist:
            return Response({'message': 'User not found'})

        user.set_password(password)
        user.save()
        return Response({'message': 'Password reset success'})
