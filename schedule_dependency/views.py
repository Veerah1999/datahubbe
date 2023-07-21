from rest_framework.views import APIView  
from rest_framework.response import Response     
from rest_framework import status
from datahub_v3_app.models import ScheduleDependency 
from schedule_dependency.serializers import Schedule_Dependency_Serializer
# from rest_framework.permissions import IsAuthenticated



#var --> Variable
class Schedule_Dependency_View(APIView):
    # permission_classes = (IsAuthenticated,)
    def get_user_by_pk(self, pk):
        try:
            return ScheduleDependency.objects.get(pk=pk)
        except:
            return Response({
                'error': 'does not exist'
            }, status=status.HTTP_404_NOT_FOUND)

    def get(self, request, pk=None):

        if pk:
                var_sch = self.get_user_by_pk(pk)
                var_serializer = Schedule_Dependency_Serializer(var_sch)
                return Response([var_serializer.data])

        else:
                var_sch = ScheduleDependency.objects.all()
                var_serializer = Schedule_Dependency_Serializer(var_sch, many=True)
                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer =Schedule_Dependency_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Created Successfully',
            'data': var_serializer.data
        }

        return response

    def put(self, request, pk=None, format=None):
        var_update_schedule = ScheduleDependency.objects.get(pk=pk)
        var_serializer = Schedule_Dependency_Serializer(instance=var_update_schedule,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'Updated Successfully',
            'data': var_serializer.data
        }

        return response
    def delete(self, request, pk, format=None):
        var_delete_schedule =  ScheduleDependency.objects.get(pk=pk)

        var_delete_schedule.delete()

        return Response({
            'message': 'Deleted Successfully'
        })





