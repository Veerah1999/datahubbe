# from urllib import response
# from rest_framework.views import APIView
# from rest_framework.permissions import IsAuthenticated, AllowAny
# from rest_framework.response import Response
# from rest_framework.exceptions import AuthenticationFailed
# # from login_api.serializers import UserSerializer, LoginSerializer
# from datahub_v3_app.models import User
# import jwt, datetime
# from rest_framework import status


# class Login_View(APIView):
#     def post(self, request):
#         var_email = request.data['email']
#         var_password = request.data['password']

#         var_user = User.objects.filter(email=var_email).first()

#         if var_user is None:
#             raise AuthenticationFailed('User not found!')

#         if not var_user.check_password(var_password):
#             raise AuthenticationFailed('Incorrect password!')

#         payload = {
#             'id': var_user.id,
#             'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
#             'iat': datetime.datetime.utcnow()
#         }

#         var_token = jwt.encode(payload,'secret', algorithm='HS256').decode('utf-8')

#         return Response({
#             'jwt': var_token,
#             'id': var_user.id
#         })
from urllib import response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework.exceptions import AuthenticationFailed
# from login_api.serializers import UserSerializer, LoginSerializer
from datahub_v3_app.models import User
import jwt, datetime
from rest_framework import status
# from django.utils.decorators import method_decorator
# from django.views.decorators.csrf import csrf_exempt


class Login_View(APIView):
    # @method_decorator(csrf_exempt)
    def post(self, request):
        email = request.data['email']
        password = request.data['password']

        user = User.objects.filter(email=email).first()

        if user is None:
            raise AuthenticationFailed('User not found!')

        if not user.check_password(password):
            raise AuthenticationFailed('Incorrect password!')

        payload = {
            'id': user.id,
            'email': user.email,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            'iat': datetime.datetime.utcnow()
        }

        token = jwt.encode(payload,'secret', algorithm='HS256')

        return Response({
            'jwt': token,
            'id': user.id
        })
    
# class user_view(APIView):
#     def get(self,request):
#         token = request.COOKIES.get('jwt')
#         return Response(token)