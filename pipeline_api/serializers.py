from rest_framework import serializers
from datahub_v3_app.models import pipeline

class Pipeline_Serializer(serializers.ModelSerializer):

    class Meta:
        model =  pipeline
        fields = ['id','pipeline_name','configuration_name','email','Description','Start_date','End_date','is_active','config_id']