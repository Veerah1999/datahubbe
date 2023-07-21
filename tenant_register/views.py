from django.shortcuts import render
import email
from urllib import response
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from tenant_register.serializers import Tenant_User_Serializer
from datahub_v3_app.models import tenant_user
from rest_framework import status
import logging
from django.http.response import Http404

logger = logging.getLogger("mylogger")


class Tenant_Register_View(APIView):
    serializer_class = Tenant_User_Serializer
    def get_object(self, pk):
            try:
                return tenant_user.objects.get(pk=pk)
            except tenant_user.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Tenant_User_Serializer(data)
                return Response([var_serializer.data])

            else:
                data = tenant_user.objects.all()
                var_serializer = Tenant_User_Serializer(data, many=True)

                return Response(var_serializer.data)
    
    def post(self, request):
        var_serializer = Tenant_User_Serializer(data=request.data)
     
        if var_serializer.is_valid(raise_exception=True):
            var_serializer.save()
            response = {
                "message": "Register Successfully",
                "data": var_serializer.data
            }
            return Response(data=response, status=status.HTTP_201_CREATED)  
        return Response(data=response, status=status.HTTP_400_BAD_REQUEST)

    def put(self, request, pk=None, format=None):
        var_update_pipeline_det= tenant_user.objects.get(pk=pk)
        var_serializer = Tenant_User_Serializer(instance=var_update_pipeline_det,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Updated Successfully',
            'data': var_serializer.data
        }
        return response(var_serializer.data)
