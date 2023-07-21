from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.views import APIView
from urllib import response
from django.http.response import Http404
from  datahub_v3_app.models import pages
from .serializers import *

# Create your views here.
class Page_View(APIView):
    def get_object(self, pk):
            try:
                return pages.objects.get(pk=pk)
            except pages.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Page_Serializer(data)
                return Response(var_serializer.data)

            else:
                data = pages.objects.all()
                var_serializer = Page_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = Page_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Created Successfully',
            'data': var_serializer.data
        }

        return response

    def put(self, request, pk=None, format=None):
        var_update_page = pages.objects.get(pk=pk)
        var_serializer = Page_Serializer(instance=var_update_page,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Updated Successfully',
            'data': var_serializer.data
        }

        return response
    
        
    def delete(self, request, pk, format=None):
        var_delete_page =  pages.objects.get(pk=pk)

        var_delete_page.delete()

        return Response({
            'message': ' Deleted Successfully'
        })
