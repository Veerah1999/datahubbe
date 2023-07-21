from django.shortcuts import render
from ast import Delete
from django.http.response import Http404
from urllib import response
from django.shortcuts import render
from rest_framework import generics
from datahub_v3_app.models import user_api
from rest_framework.views import APIView
from user_api.serializers import user_serializer
from rest_framework.response import Response



# Create your views here.

class User_Profile_View(APIView):
    def get_object(self, pk):
            try:
                return user_api.objects.get(pk=pk)
            except user_api.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                serializer = user_serializer(data)
                return Response(serializer.data)
            else:
                data = user_api.objects.all()
                serializer = user_serializer(data, many=True)

                return Response(serializer.data)

    def post(self, request, format=None):
        serializer = user_serializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=200)
        return Response(serializer.errors, status=400)


    def put(self, request, pk=None, format=None):
        conn_to_update = user_api.objects.get(pk=pk)
        serializer = user_serializer(instance=conn_to_update,data=request.data, partial=True)

        serializer.is_valid(raise_exception=True)

        serializer.save()

        response = Response()

        response.data = {
            'message': 'Updated Successfully',
            'data': serializer.data
        }

        return response
    def delete(self, request, pk, format=None):
        todo_to_delete =  user_api.objects.get(pk=pk)

        todo_to_delete.delete()

        return Response({
            'message': 'Deleted Successfully'
        })
