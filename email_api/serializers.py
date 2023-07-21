from rest_framework import serializers
from datahub_v3_app.models import *


class Email_Serializer(serializers.ModelSerializer):

    class Meta:
        model= tenant_user        
        fields= "__all__"