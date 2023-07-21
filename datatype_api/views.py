from django.http.response import Http404
from django.shortcuts import render
from datahub_v3_app.models import datatype
from rest_framework.views import APIView
from datatype_api.serializers import Datatype_Serializer
from rest_framework.response import Response


# Create your views here.

class Datatype_View(APIView):
    def get_object(self, pk):
            try:
                return datatype.objects.get(pk=pk)
            except datatype.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Datatype_Serializer(data)
                return Response([var_serializer.data])

            else:
                data = datatype.objects.all()
                var_serializer = Datatype_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data

        var_serializer = Datatype_Serializer(data=data)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Datatype Created Successfully',
            'data': var_serializer.data
            }

        return response

    def put(self, request, pk=None, format=None):
        var_update_datatype = datatype.objects.get(pk=pk)
        var_serializer = Datatype_Serializer(instance=var_update_datatype,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Datatype Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_daratype = datatype.objects.get(pk=pk)
        var_delete_daratype.delete()
        return Response({
            'message': 'Datatype Deleted Successfully'
        })