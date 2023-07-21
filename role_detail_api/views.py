from django.shortcuts import render
from django.http.response import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from datahub_v3_app.models import *
from role_detail_api.serializers import *

# Create your views here.
class Role_detail_View(APIView):
    def get_object(self, pk):
            try:
                return role_details_api.objects.get(pk=pk)
            except role_details_api.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Role_Detail_Serializer(data)
                return Response(var_serializer.data)

            else:
                data = role_details_api.objects.all()
                var_serializer = Role_Detail_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = Role_Detail_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Created Successfully',
            'data': var_serializer.data
        }

        return response

    def put(self, request, pk=None, format=None):
        var_update_role_detail = role_details_api.objects.get(pk=pk)
        var_serializer = Role_Detail_Serializer(instance=var_update_role_detail,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Updated Successfully',
            'data': var_serializer.data
        }

        return response
    def delete(self, request, pk, format=None):
        var_delete_role_detail =  role_details_api.objects.get(pk=pk)

        var_delete_role_detail.delete()
        return Response({
            'message': ' Deleted Successfully'
        })