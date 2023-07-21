import email
from urllib import response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from register_api.serializers import User_Serializer
from datahub_v3_app.models import User
from rest_framework import status
import logging

logger = logging.getLogger("mylogger")




class RegisterView(APIView):
    serializer_class = User_Serializer
    def post(self, request):
        var_serializer = User_Serializer(data=request.data)
     
        if var_serializer.is_valid(raise_exception=True):
            var_serializer.save()
            response = {
                "message": "Register Successfully",
                "data": var_serializer.data
            }
            return Response(data=response, status=status.HTTP_201_CREATED)  
        return Response(data=response, status=status.HTTP_400_BAD_REQUEST) 
      