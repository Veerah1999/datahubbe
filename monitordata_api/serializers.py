from rest_framework import serializers
from datahub_v3_app.models import *


class Schedule_Log_Serializer(serializers.ModelSerializer):

    class Meta:
        model= schedule_log
        fields = '__all__'


class Audit_log_Serializer(serializers.ModelSerializer):

    class Meta:
        model =  audit_log
        fields = '__all__'

class Schema_Log_Serializer(serializers.ModelSerializer):

    class Meta:
        model =  schema_log
        fields = '__all__'

class Schema_audit_Log_Serializer(serializers.ModelSerializer):

    class Meta:
        model =  schema_audit_log
        fields = '_all_'
        
class Schema_error_Log_Serializer(serializers.ModelSerializer):

    class Meta:
        model =  schema_error_log
        fields = '_all_'
