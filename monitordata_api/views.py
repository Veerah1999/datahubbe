from django.http.response import Http404
from datahub_v3_app.models import *
from rest_framework.views import APIView
from rest_framework.response import Response
from monitordata_api.serializers import *

class Schema_Monitor_View(APIView):
     def get_object(self, pk):
            try:
                return schema_log.objects.get(pk=pk)
            except schema_log.DoesNotExist:
                raise Http404
     def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                serializer = Schema_Log_Serializer(data)
                return Response(serializer.data)

            else:
                data = schema_log.objects.all()
                serializer = Schema_Log_Serializer(data, many=True)

                return Response(serializer.data)

class Audit_Monitor_View(APIView):
    def get_object(self, k):
            try:
                return audit_log.objects.filter(schedule_id=k).values()
            except audit_log.DoesNotExist:
                raise Http404
    def get(self, request, k=None, format=None):
            if k:
                data = self.get_object(k)
                serializer = Audit_log_Serializer(data,many=True)
                return Response(serializer.data)

            else:
                data = audit_log.objects.all()
                serializer = Audit_log_Serializer(data, many=True)

                return Response(serializer.data)

class Schedule_Monitor_View(APIView):
     def get_object(self, pk):
            try:
                return schedule_log.objects.get(pk=pk)
            except schedule_log.DoesNotExist:
                raise Http404
     def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                serializer = Schedule_Log_Serializer(data)
                return Response(serializer.data)

            else:
                data = schedule_log.objects.all()
                serializer = Schedule_Log_Serializer(data, many=True)

                return Response(serializer.data)


class Schema_audit_Monitor_View(APIView):
     def get_object(self, pk):
            try:
                return schema_audit_log.objects.filter(run_ids=pk)
            except schema_audit_log.DoesNotExist:
                raise Http404
     def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                serializer = Schema_audit_Log_Serializer(data)
                return Response(serializer.data)

            else:
                data = schema_audit_log.objects.all()
                serializer = Schema_audit_Log_Serializer(data, many=True)

                return Response(serializer.data)

class Schema_error_Monitor_View(APIView):
     def get_object(self, pk):
            try:
                return schema_error_log.objects.filter(run_ids=pk)
            except schema_error_log.DoesNotExist:
                raise Http404
     def get(self, request, pk=None, format=None):
            if pk:
                data = self.get_object(pk)
                serializer = Schema_error_Log_Serializer(data)
                return Response(serializer.data)

            else:
                data = schema_error_log.objects.all()
                serializer = Schema_error_Log_Serializer(data, many=True)

                return Response(serializer.data)
