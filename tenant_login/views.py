from urllib import response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
# from login_api.serializers import UserSerializer, LoginSerializer
from datahub_v3_app.models import *
import jwt, datetime
from rest_framework import status
from urllib import response
from rest_framework.views import APIView
# from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
# from login_api.serializers import UserSerializer, LoginSerializer
# from login_app.models import User
import jwt, datetime
from rest_framework import status


class ten_login_view(APIView):
    def post(self, request):
        # import pdb
        # pdb.set_trace
        email = request.data['email']
        password = request.data['password']

        user = tenant_user.objects.filter(email=email).first()

        if user is None:
            raise AuthenticationFailed('User not found!')

        if not user.check_password(password):
            raise AuthenticationFailed('Incorrect password!')

        payload = {
            'id': user.id,
            'email':user.email,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            'iat': datetime.datetime.utcnow()
        }

        token = jwt.encode(payload,'secret', algorithm='HS256') #.decode('utf-8')

        return Response({

            'jwt':token,
            'id':user.id,
           
        })
