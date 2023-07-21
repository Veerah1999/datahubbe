from django.shortcuts import render
from rest_framework.generics import ListAPIView
from rest_framework.generics import CreateAPIView
from rest_framework.generics import DestroyAPIView
from rest_framework.generics import UpdateAPIView
from datahub_v3_app.models import conn
from rest_framework import status
from connection_api.serializers import Connection_Serializer
from rest_framework.response import Response
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import permissions ,filters
from rest_framework.permissions import IsAuthenticated


class List_Connections_View(ListAPIView):
    queryset = conn.objects.all()
    serializer_class = Connection_Serializer
    

    var_filter_backends = [DjangoFilterBackend,filters.SearchFilter,filters.OrderingFilter]

    var_filterset_fields = ['id','connection_name']
    var_search_fields = ['connection_name']
    var_ordering_fields = ['id', 'connection_name']
    def get_user_by_pk(self, pk):
        try:
            return conn.objects.get(pk=pk)
        except:
            return Response({
                'error': 'does not exist'
            }, status=status.HTTP_404_NOT_FOUND)

    def get(self, request, pk=None):

        if pk:
                var_conn = self.get_user_by_pk(pk)
                var_serializer = Connection_Serializer(var_conn)
                return Response([var_serializer.data])

        else:
                var_reg = conn.objects.all()
                var_serializer = Connection_Serializer(var_reg, many=True)
                return Response(var_serializer.data)

class Create_Connections_View(CreateAPIView):
    # permission_classes = (IsAuthenticated)
    queryset = conn.objects.all()
    serializer_class = Connection_Serializer

   
class Update_Connections_View(UpdateAPIView):
    queryset = conn.objects.all()
    serializer_class = Connection_Serializer

    def put(self, request, pk=None, format=None):
        var_update_connection = conn.objects.get(pk=pk)
        var_serializer = Connection_Serializer(instance=var_update_connection,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'connections Updated Successfully',
            'data': var_serializer.data
        }

        return response

class Delete_Connections_View(DestroyAPIView):
    queryset = conn.objects.all()
    serializer_class = Connection_Serializer

    def delete(self, request, pk, format=None):
        var_delete_connection =  conn.objects.get(pk=pk)

        var_delete_connection.delete()

        return Response({
            'message': 'connections Deleted Successfully'
        })