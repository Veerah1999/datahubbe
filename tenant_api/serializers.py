from datahub_v3_app.models import *
from rest_framework import serializers


class tenant_serializer(serializers.ModelSerializer):
    class Meta:
        model = tenant_user
        fields = '__all__'


