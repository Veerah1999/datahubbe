from datahub_v3_app.models import db_sql_table
from rest_framework.views import APIView
from db_sql_extract.serializers import dbextract_Serializer
from rest_framework.response import Response
from django.http.response import Http404
from rest_framework import status
from rest_framework.generics import CreateAPIView
# from rest_framework.permissions import IsAuthenticated


# Create your views here.       

class Sql_Extract_View(APIView):
    # permission_classes = (IsAuthenticated,)
    def get_user_by_pk(self, pk):
        try:
            return db_sql_table.objects.get(pk=pk)
        except:
            return Response({
                'error': 'does not exist'
            }, status=status.HTTP_404_NOT_FOUND)

    def get(self, request, pk=None):

        if pk:
                var_dbsql = self.get_user_by_pk(pk)
                var_serializer = dbextract_Serializer(var_dbsql)
                return Response([var_serializer.data])

        else:
                var_dbsql = db_sql_table.objects.all()
                var_serializer = dbextract_Serializer(var_dbsql, many=True)
                return Response(var_serializer.data)


    def post(self, request, format=None):
        data = request.data
        var_serializer = dbextract_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Created Successfully',
            'data': var_serializer.data
        }

        return response

    def put(self, request, pk=None, format=None):
        var_update_sqlextract = db_sql_table.objects.get(pk=pk)
        serializer = dbextract_Serializer(instance=var_update_sqlextract,data=request.data, partial=True)

        serializer.is_valid(raise_exception=True)

        serializer.save()

        response = Response()

        response.data = {
            'message': ' Updated Successfully',
            'data': serializer.data
        }

        return response
    def delete(self, request, pk, format=None):
        var_delete_sqlextract =  db_sql_table.objects.get(pk=pk)

        var_delete_sqlextract.delete()

        return Response({
            'message': ' Deleted Successfully'
        })
