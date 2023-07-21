from django.http.response import Http404
from datahub_v3_app.models import pipeline_details
from rest_framework.views import APIView
from pipeline_details_api.serializers import Pipeline_Detail_Serializer
from rest_framework.response import Response



class Pipeline_Detail_View(APIView):
    def get_object(self, pk):
            try:
                return pipeline_details.objects.get(pk=pk)
            except pipeline_details.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Pipeline_Detail_Serializer(data)
                return Response([var_serializer.data])

            else:
                data = pipeline_details.objects.all()
                var_serializer = Pipeline_Detail_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data

        var_serializer = Pipeline_Detail_Serializer(data=data)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Pipeline detail Created Successfully',
            'data': var_serializer.data
            }

        return response

    def put(self, request, pk=None, format=None):
        var_update_pipeline_det= pipeline_details.objects.get(pk=pk)
        var_serializer = Pipeline_Detail_Serializer(instance=var_update_pipeline_det,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Pipeline detail Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_pipeline_det =  pipeline_details.objects.get(pk=pk)
        var_delete_pipeline_det.delete()
        return Response({
            'message': 'Pipeline detail Deleted Successfully'
        })