import datetime
from django.shortcuts import render
from apscheduler.schedulers.background import BackgroundScheduler
from rest_framework.response import Response
from rest_framework.views import APIView

from datahub_v3_project.sche_task import job
from  datahub_v3_app.models import schedule_jobs
from .serializers import Schedule_Serializer

class Schdule_View(APIView):

    def post(self, request, format=None,pk=None):
        data = request.data
        var_serializer = Schedule_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_m=var_serializer.save()
        var_std = datetime.datetime.now()
        var_sec=var_m.seconds
        var_hrs=var_m.hours
        var_mit=var_m.minutes
        var_etd=var_m.end_date
        var_run_imm=var_m.run_imm
        print(id)
        var_scheduler = BackgroundScheduler()
        # scheduler.start()
        if var_run_imm:
            var_scheduler = BackgroundScheduler()
            var_scheduler.add_job(job)
            var_scheduler.start()


        response = Response()

        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': var_serializer.data
        }

        return response
