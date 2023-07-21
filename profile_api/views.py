from django.shortcuts import render
from rest_framework.views import APIView
#from.models import pipe_line
#from .serializers import pipe_lineserializer
from django.http.response import Http404
from urllib import response
from rest_framework.response import Response
import requests
from datahub_v3_app.models import User
from profile_api.serializers import Profile_Serializer

class Profile_View(APIView):
    def get_object(self, pk):
            try:
                return User.objects.get(pk=pk)
            except User.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Profile_Serializer(data)
                return Response(var_serializer.data)

            else:
               data = User.objects.all()
               var_serializer = Profile_Serializer(data, many=True)

               return Response(var_serializer.data)
            
    def put(self, request, pk=None, format=None):
        var_update_profile = User.objects.get(pk=pk)
        var_serializer = Profile_Serializer(instance=var_update_profile,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Updated Successfully',
            'data': var_serializer.data
        
        }
        return response