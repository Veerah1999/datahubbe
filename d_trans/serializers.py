from rest_framework import serializers
from datahub_v3_app.models import *


class d_transform_Serializer(serializers.ModelSerializer):

    class Meta:
        model= d_transform
        fields= "__all__"
