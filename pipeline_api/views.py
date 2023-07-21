from django.http.response import Http404
from datahub_v3_app.models import pipeline
from rest_framework.views import APIView
from pipeline_api.serializers import Pipeline_Serializer
from rest_framework.response import Response
# from rest_framework.permissions import IsAuthenticated, AllowAny
# from rest_framework.authentication import TokenAuthentication,SessionAuthentication,BasicAuthentication
# from rest_framework_api_key.permissions import HasAPIKey



class Pipeline_View(APIView):
    # authentication_classes = (SessionAuthentication, BasicAuthentication, TokenAuthentication)
    # queryset = pipeline.objects.all()
    # serializer_class = Pipeline_Serializer
    # permission_classes = (IsAuthenticated,)
    def get_object(self, pk):
            try:
                return pipeline.objects.get(pk=pk)
            except pipeline.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Pipeline_Serializer(data)
                return Response([var_serializer.data])

            else:
                data = pipeline.objects.all()
                var_serializer = Pipeline_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data

        var_serializer = Pipeline_Serializer(data=data)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Created Successfully',
            'data': var_serializer.data
            }

        return response

    def put(self, request, pk=None, format=None):
        var_update_pipeline = pipeline.objects.get(pk=pk)
        var_serializer = Pipeline_Serializer(instance=var_update_pipeline,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_pipeline = pipeline.objects.get(pk=pk)
        var_delete_pipeline.delete()
        return Response({
            'message': 'Deleted Successfully'
        })