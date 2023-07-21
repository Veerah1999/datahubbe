import datetime

from django.http import Http404
from django.shortcuts import render
from rest_framework import response

# Create your views here.
#from apscheduler.schedulers.background import BackgroundScheduler
from rest_framework.response import Response
from rest_framework.templatetags.rest_framework import data
from rest_framework.views import APIView
from datetime import datetime
from price_calculator.serializers import price_Serializer
import boto3
import json
from pkg_resources import resource_filename
import boto3
import json
from pkg_resources import resource_filename
from datahub_v3_app.models import *
from rest_framework import status






class price(APIView):
         def get_user_by_pk(self, pk):
          try:
            return price_tables.objects.get(pk=pk)
          except:
            return Response({
                'error': 'does not exist'
            }, status=status.HTTP_404_NOT_FOUND)

         def get(self, request, pk=None):

            if pk:
                    var_dbsql = self.get_user_by_pk(pk)
                    var_serializer = price_Serializer(var_dbsql)
                    return Response([var_serializer.data])

            else:
                    var_dbsql = price_tables.objects.all()
                    var_serializer = price_Serializer(var_dbsql, many=True)
                    return Response(var_serializer.data)


         def post(self, request, format=None):
            data = request.data
            var_serializer = price_Serializer(data=data)

            var_serializer.is_valid(raise_exception=True)

            var_serializer.save()

            response = Response()

            response.data = {
                'message': ' Created Successfully',
                'data': var_serializer.data
            }

            return response

         def put(self, request, pk=None, format=None):
            var_update_sqlextract = price_tables.objects.get(pk=pk)
            serializer = price_Serializer(instance=var_update_sqlextract,data=request.data, partial=True)

            serializer.is_valid(raise_exception=True)

            serializer.save()

            response = Response()

            response.data = {
                'message': ' Updated Successfully',
                'data': serializer.data
            }

            return response
         def delete(self, request, pk, format=None):
            var_delete_sqlextract =  price_tables.objects.get(pk=pk)

            var_delete_sqlextract.delete()

            return Response({
                'message': ' Deleted Successfully'
            })