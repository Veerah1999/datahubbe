from datahub_v3_app.models import *
from rest_framework.views import APIView
from google_login_api.serializers import *
from rest_framework.response import Response
from schema_framework.views import *

class google_api(APIView):
    def get(self, request, pk=None):
            if pk:
                data = google_login.objects.get(pk=pk)
                var_serializer = google_serializers(data)
                return Response(var_serializer.data)
            else:
                data = google_login.objects.all()
                var_serializer = google_serializers(data, many=True)

                return Response(var_serializer.data)

    def post(self,request):
        data = request.data
        serializer = google_serializers(data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        response = Response()

        response.data = {
            'message': 'Created Sucessfully',
            'data': serializer.data
        }
        return response

    def put(self,request,pk):
        var_update = google_login.objects.get(pk=pk)
        var_update = google_serializers(instance=var_update,data=request.data, partial=True)

        var_update.is_valid(raise_exception=True)

        var_update.save()

        response = Response()

        response.data = {
            'message': 'Updated Successfully',
            'data': var_update.data
        }

        return response
    
    def delete(self, request, pk, format=None):
        var_delete_schema =  google_login.objects.get(pk=pk)

        var_delete_schema.delete()

        return Response({
            'message': 'Schema Deleted Successfully'
        })
