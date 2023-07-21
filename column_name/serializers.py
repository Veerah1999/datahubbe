from rest_framework import serializers
from datahub_v3_app.models import column_name

class column_namee(serializers.ModelSerializer):
    class Meta:
        model = column_name
        fields = "__all__"
