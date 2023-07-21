from urllib import response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
# from login_api.serializers import UserSerializer, LoginSerializer
from datahub_v3_app.models import *
import jwt, datetime
# from login_api.views import Login_View
from rest_framework import status

class Security_View(APIView):
   def post(self, request):
        # import pdb
        # pdb.set_trace()
      
        var_jwt_new = request.data["authentication"]
        
        var_jwt_new1 = jwt.decode(var_jwt_new, 'secret', algorithms="HS256")
         
        var_email = var_jwt_new1.get('email')
        user = User.objects.filter(email=var_email).exists()
        
        # email={}

        if  User.objects.filter(email=var_email).exists():
           
       
      
            # var_email == .email
          # if user == User.objects.get(email=var_email):
            var_new = True
            payload = {
               'email': var_email,
               'status':var_new,
               'role':'user',
              }
            var_token_ = jwt.encode(payload, 'secret', algorithm='HS256')
            return Response({
              'verifyToken': var_token_,
              'status':var_new,
              'role':'user',
                })
        else :
            var_jwt_new1 = jwt.decode(var_jwt_new, 'secret', algorithms="HS256")
            var_email = var_jwt_new1.get('email')
            user2= tenant_user.objects.get(email=var_email)
            vare=True
            payload= {
                'email':user2.email,
                'status':vare,
                'role':user2.role
              }
            vare_token = jwt.encode(payload,'secret',algorithm='HS256')
            return Response({
                'verifyToken':vare_token,
                'status':vare,
                'role':user2.role})
