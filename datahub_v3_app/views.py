# from urllib import response
# from rest_framework.views import APIView
# from rest_framework.permissions import IsAuthenticated, AllowAny
# from rest_framework.response import Response
# from rest_framework.exceptions import AuthenticationFailed
# from .serializers import UserSerializer
# from .models import User
# import jwt, datetime
# from rest_framework import status



# class RegisterView(APIView):
#     serializer_class = UserSerializer
#     def post(self, request):
#         serializer = UserSerializer(data=request.data)
     
#         if serializer.is_valid(raise_exception=True):
#             serializer.save()
#             response = {
#                 "message": "Register Successfully",
#                 "data": serializer.data
#             }
#             return Response(data=response, status=status.HTTP_201_CREATED)  
#         return Response(data=response, status=status.HTTP_400_BAD_REQUEST)  
    
                



# class LoginView(APIView):
#     def post(self, request):
#         email = request.data['email']
#         password = request.data['password']

#         user = User.objects.filter(email=email).first()

#         if user is None:
#             raise AuthenticationFailed('User not found!')

#         if not user.check_password(password):
#             raise AuthenticationFailed('Incorrect password!')

#         payload = {
#             'id': user.id,
#             'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
#             'iat': datetime.datetime.utcnow()
#         }

#         token = jwt.encode(payload, 'secret', algorithm='HS256').decode('utf-8')

#         response = Response()

#         response.set_cookie(key='jwt', value=token, httponly=True)
#         response.data = {
#             'jwt': token
#         }
#         return response




