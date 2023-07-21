from rest_framework import serializers
from datahub_v3_app.models import conn

class Connection_Serializer(serializers.ModelSerializer):
    class Meta:
        model = conn
        fields = "__all__"