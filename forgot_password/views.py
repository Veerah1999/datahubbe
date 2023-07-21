from rest_framework.views import APIView
import random      # generate a random number
from rest_framework.response import Response
from .email import *
from datahub_v3_app.models import *


    
class fpassword(APIView):
    def post(self,request):
        print("creating obj")
        email_id=request.data['email'] 
        
        try:
            use=User.objects.get(email=email_id) 
        except :
            try:
                use=tenant_user.objects.get(email=email_id)
            except:
                return Response({"message":"email not found"},status = 403)
        n=random.randrange(100000,999999)     #generating a random number
        otp=n 
        print(otp,"otp")
        message=f"""Greetings  from Datahub
                    OTP for changing your password is
                    {otp}
                    
                    Have a Good day
                    
                    

                    Thankyou
                    Data hub
                    """
        print("31")  
                
        email_trigger.sendemail(email_id,message)
        print("33")
        for_pass=f_pass.objects.create(email=email_id,otp=otp)
        return Response({"message":"email sent successfull......"})
    
        

class verify(APIView):
    def post(self,request):
        try:
            getobj=f_pass.objects.filter(email=request.data['email']).last()
            if getobj.otp==request.data['otp']:
                getobj.delete()
                return Response({"message":"otp verified"})
            else:
                return Response({"message":"wrong otp"},status = 403)
            
        except:
            return Response({"message":"otp not exist"},status = 403)


class reset(APIView):
    def post(self, request):

            try:
                use = tenant_user.objects.get(email=request.data['email'])
            except :
                try:
                    use = User.objects.get(email=request.data['email'])
                except:
                    return Response({"message": "email not found"},status = 403)
            

            use.set_password(request.data['password'])
            use.save()
            return Response({'message': 'Password reset success'})
