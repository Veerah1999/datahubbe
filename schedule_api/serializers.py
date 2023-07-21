from rest_framework import serializers
from datahub_v3_app.models import schedule_jobs

class Schedule_Serializer(serializers.ModelSerializer):

    class Meta:
        model =  schedule_jobs
        fields = '__all__'