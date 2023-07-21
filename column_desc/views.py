from django.http.response import Http404
from datahub_v3_app.models import column_config
from rest_framework.views import APIView
from column_desc.serializers import column_desc
from rest_framework.response import Response



class column_description(APIView):
    def get_object(self, pk):
            try:
                return column_config.objects.get(pk=pk)
            except column_config.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
        if pk:
                data = self.get_object(pk)
                var_serializer = column_desc(data)
                return Response([var_serializer.data])

        else:
                data = column_config.objects.all()
                var_serializer = column_desc(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = column_desc(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': var_serializer.data
        }
        return response

    def put(self, request, pk=None, format=None):
        var_update_conn_details = column_config.objects.get(pk=pk)
        var_serializer = column_desc(instance=var_update_conn_details,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'conect_detail Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_conn_details =  column_config.objects.get(pk=pk)
        var_delete_conn_details.delete()
        return Response({
            'message': 'connect_detail Deleted Successfully'
        })


