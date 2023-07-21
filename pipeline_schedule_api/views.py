from django.http.response import Http404
from datahub_v3_app.models import pipeline_schedule
from rest_framework.views import APIView
from pipeline_schedule_api.serializers import Pipeline_Schedule_Serializer
from rest_framework.response import Response
import datetime
from datahub_v3_project.task import sample_function
from pipeline_framework.views import *


class Pipeline_Schedule_View(APIView):
    def get_object(self, pk):
            try:
                return pipeline_schedule.objects.get(pk=pk)
            except pipeline_schedule.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                var_pipe_sch = self.get_object(pk)
                var_serializer = Pipeline_Schedule_Serializer(var_pipe_sch)
                return Response([var_serializer.data])
            else:
                var_pipe_sch = pipeline_schedule.objects.all()
                var_serializer = Pipeline_Schedule_Serializer(var_pipe_sch, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data

        var_serializer = Pipeline_Schedule_Serializer(data=data)
        var_serializer.is_valid(raise_exception=True)
        pass_set=var_serializer.save()
        set = {"id":pass_set.id}
        response = Response()
        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': set
        }
        return response
       


    
    def put(self, request, pk=None, format=None):
        var_update_pipelineschedule = pipeline_schedule.objects.get(pk=pk)
        var_serializer = Pipeline_Schedule_Serializer(instance=var_update_pipelineschedule,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'Pipeline Schedule Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_pipelineschedule =  pipeline_schedule.objects.get(pk=pk)

        var_delete_pipelineschedule.delete()

        return Response({
            'message': 'Pipeline Schedule Deleted Successfully'
        })

class table_trigger(APIView):

    def get(self,request,pk):
       # import pdb 
       # pdb.set_trace()
        print(pk)
        main(sch_id=pk)
        return Response("Done")
