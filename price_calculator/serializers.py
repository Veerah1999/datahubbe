from rest_framework import serializers
from datahub_v3_app.models import price_tables

class price_Serializer(serializers.ModelSerializer):
    class Meta:
        model = price_tables
        fields = "__all__"