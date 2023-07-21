from django.http.response import Http404
from datahub_v3_app.models import d_transform
from rest_framework.views import APIView
from .serializers import *
from rest_framework.response import Response
#from rest_framework.permissions import IsAuthenticated
#from rest_framework_simplejwt.authentication import JWTAuthentication

# Create your views here.

class d_trans_View(APIView):
   # permission_classes = (IsAuthenticated, )
   # authentication_classes = [JWTAuthentication]
    def get_object(self, pk):
            try:
                return d_transform.objects.get(pk=pk)
            except d_transform.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = d_transform_Serializer(data)
                return Response(var_serializer.data)

            else:
                data = d_transform.objects.all()
                var_serializer = d_transform_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = d_transform_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': var_serializer.data
        }

        return response

    def put(self, request, pk=None, format=None):
        var_update_dbconfig = d_transform.objects.get(pk=pk)
        var_serializer = d_transform_Serializer(instance=var_update_dbconfig,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'conect_detail Updated Successfully',
            'data': var_serializer.data
        }

        return response

    def delete(self, request, pk, format=None):
        var_delete_dbconfig =d_transform.objects.get(pk=pk)

        var_delete_dbconfig.delete()

        return Response({
            'message': 'connect_detail Deleted Successfully'
        })

