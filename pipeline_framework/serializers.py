from rest_framework import serializers
from datahub_v3_app.models import *


class schedule_logserializer(serializers.ModelSerializer):

    class Meta:
        model= schedule_log
        fields = ['run_id','schedule_id','start_time','status']

class audit_logserializer(serializers.ModelSerializer):

    class Meta:
        model= audit_log
        fields = ['audit_id','run_id','schedule_id','start_time','status']