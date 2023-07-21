from dataclasses import fields
from datahub_v3_app.models import users_role_view
from django.shortcuts import render
from rest_framework.views import APIView
from django.http.response import Http404
from rest_framework.response import Response
from user_role_api.serializers import Users_Role_Serializer

class User_Role_View(APIView):
    def get_object(self, pk):
            try:
                return users_role_view.objects.get(pk=pk)
            except users_role_view.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                var_serializer = Users_Role_Serializer(data)
                return Response(var_serializer.data)

            else:
                data = users_role_view.objects.all()
                var_serializer = Users_Role_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = Users_Role_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Created Successfully',
            'data': var_serializer.data
        }

        return response

    def put(self, request, pk=None, format=None):
        var_update_user_role = users_role_view.objects.get(pk=pk)
        var_serializer = Users_Role_Serializer(instance=var_update_user_role,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': ' Updated Successfully',
            'data': var_serializer.data
        }

        return response
    def delete(self, request, pk, format=None):
        var_delete_user_role =  users_role_view.objects.get(pk=pk)

        var_delete_user_role.delete()

        return Response({
            'message': ' Deleted Successfully'
        })