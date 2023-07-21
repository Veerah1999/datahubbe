from ast import Delete
from django.http.response import Http404
from urllib import response
from django.shortcuts import render
from rest_framework import generics
from datahub_v3_app.models import sql_generator
from rest_framework.views import APIView
from sql_generator.serializers import SQL_Generator_Serializer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated



class SQL_Generator_View(APIView):
    def get_object(self, pk):
            try:
                return sql_generator.objects.get(pk=pk)
            except sql_generator.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = SQL_Generator_Serializer(data)
                return Response([var_serializer.data])

            else:
                data = sql_generator.objects.all()
                var_serializer = SQL_Generator_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data

        var_serializer = SQL_Generator_Serializer(data=data)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Created Successfully',
            'data': var_serializer.data
            }

        return response

    def put(self, request, pk=None, format=None):
        var_update_sql_generator = sql_generator.objects.get(pk=pk)
        var_serializer = SQL_Generator_Serializer(instance=var_update_sql_generator,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_sql_generator = sql_generator.objects.get(pk=pk)
        var_delete_sql_generator.delete()
        return Response({
            'message': 'Deleted Successfully'
        })