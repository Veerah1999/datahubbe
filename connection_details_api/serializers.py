from dataclasses import fields
from rest_framework import serializers
from datahub_v3_app.models import connection_detail

class Connection_Details_Serializer(serializers.ModelSerializer):

    class Meta:
        model= connection_detail
        fields = '__all__'