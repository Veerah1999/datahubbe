# Create your views here.
from ast import Delete
from django.http.response import Http404
from urllib import response
from django.shortcuts import render
from rest_framework import generics
from datahub_v3_app.models import *
from rest_framework.views import APIView
from team_member_api.serializers import Member_Serializer,Team_Serializer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated


class Member_View(APIView):
    def get_object(self, pk):
            try:
                return member.objects.get(pk=pk)
            except member.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
        if pk:
                data = self.get_object(pk)
                var_serializer = Member_Serializer(data)
                return Response([var_serializer.data])

        else:
                data = member.objects.all()
                var_serializer = Member_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = Member_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()
        
        response = Response()

        response.data = {
            'message': ' Created Successfully',
            'data': var_serializer.data
        }
        return response

    def put(self, request, pk=None, format=None):
        var_update_member = member.objects.get(pk=pk)
        var_serializer = Member_Serializer(instance=var_update_member,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': ' Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_member =  member.objects.get(pk=pk)
        var_delete_member.delete()
        return Response({
            'message': ' Deleted Successfully'
        })

class Team_View(APIView):
    def get_object(self, pk):
            try:
                return teams_api_model.objects.get(pk=pk)
            except teams_api_model.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
        if pk:
                data = self.get_object(pk)
                var_serializer = (data)
                return Response([var_serializer.data])

        else:
                data = teams_api_model.objects.all()
                var_serializer = Team_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = Team_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()
        
        response = Response()

        response.data = {
            'message': ' Created Successfully',
            'data': var_serializer.data
        }
        return response

    def put(self, request, pk=None, format=None):
        var_update_team = teams_api_model.objects.get(pk=pk)
        var_serializer = Team_Serializer(instance=var_update_team,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': ' Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_team =  teams_api_model.objects.get(pk=pk)
        var_delete_team.delete()
        return Response({
            'message': ' Deleted Successfully'
        })