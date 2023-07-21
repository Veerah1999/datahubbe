from django.shortcuts import render
from rest_framework.views import APIView
from tenant_api.serializers import *
from rest_framework.response import Response
from team_member_api.serializers import *


class tenant_api(APIView):
    def get(self, request, pk=None):
            if pk:
                data = tenant_user.objects.get(pk=pk)
                var_serializer = tenant_serializer(data)
                return Response(var_serializer.data)
            else:
                data = tenant_user.objects.all()
                var_serializer = tenant_serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self,request):
        data = request.data
        serializer = tenant_serializer(data=data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        response = Response()

        response.data = {
            'message': 'Created Sucessfully',
            'data': serializer.data
        }
        return response

    def put(self,request,pk):
        var_update = tenant_user.objects.get(pk=pk)
        var_update = tenant_serializer(instance=var_update,data=request.data, partial=True)

        var_update.is_valid(raise_exception=True)

        var_update.save()

        response = Response()

        response.data = {
            'message': 'Schema Updated Successfully',
            'data': var_update.data
        }

        return response
    
    def delete(self, request, pk, format=None):
        var_delete_schema =  tenant_user.objects.get(pk=pk)

        var_delete_schema.delete()

        return Response({
            'message': 'Schema Deleted Successfully'
        })


class teams_api(APIView):
     def get(self, request, pk=None):
          data = teams_api_model.objects.filter(tenant_id_id = pk).values()
          var_serializers = Team_Serializer(data,many = True)
          return Response(var_serializers.data)

class member_api(APIView):
     def post(self, request, pk=None):
        new_data = request.data
        data = member.objects.filter(tenant_id_id = new_data['tenant_id_id']).filter(team_id_id = new_data['team_id_id']).values()
        var_serializers = Member_Serializer(data,many = True)
        return Response(var_serializers.data)
