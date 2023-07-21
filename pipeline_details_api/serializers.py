from rest_framework import serializers
from datahub_v3_app.models import pipeline_details


class Pipeline_Detail_Serializer(serializers.ModelSerializer):

    class Meta:
        model= pipeline_details
        fields = '__all__'