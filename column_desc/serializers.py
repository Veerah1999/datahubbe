from rest_framework import serializers
from datahub_v3_app.models import column_config

class column_desc(serializers.ModelSerializer):
    class Meta:
        model = column_config
        fields = "__all__"
