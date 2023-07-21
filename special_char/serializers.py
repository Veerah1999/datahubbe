from datahub_v3_app.models import *
from rest_framework import serializers


class spchar_serializer(serializers.ModelSerializer):
    class Meta:
        model = spl_columnchange
        fields = '__all__'
