from rest_framework import serializers
from datahub_v3_app.models import *

class google_serializers(serializers.ModelSerializer):
    class Meta:
        model = google_login
        fields = '_all_'
