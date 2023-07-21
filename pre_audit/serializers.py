from rest_framework import serializers
from datahub_v3_app.models import preaduit


class preaduitSerializer(serializers.ModelSerializer):

    class Meta:
        model= preaduit 
        fields = '__all__'