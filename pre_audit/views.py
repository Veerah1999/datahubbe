from django.http.response import Http404
from datahub_v3_app.models import preaduit
from rest_framework.views import APIView
from rest_framework.response import Response
from pre_audit.serializers import preaduitSerializer

class preauditdb(APIView):
    def get_object(self, pk):
            try:
                return preaduit.objects.get(pk=pk)
            except preaduit.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = preaduitSerializer(data)
                return Response([var_serializer.data])

            else:
                data = preaduit.objects.all()
                var_serializer = preaduitSerializer(data, many=True)

                return Response(var_serializer.data)
    def post(self, request, format=None):
        data = request.data

        var_serializer = preaduitSerializer(data=data)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
        'message': 'Created Successfully',
            'data': var_serializer.data
            }

        return response

    def put(self, request, pk=None, format=None):
        var_update_datatype = preaduit.objects.get(pk=pk)
        var_serializer = preaduitSerializer(instance=var_update_datatype,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_daratype = preaduit.objects.get(pk=pk)
        var_delete_daratype.delete()
        return Response({
            'message': 'Deleted Successfully'
        })