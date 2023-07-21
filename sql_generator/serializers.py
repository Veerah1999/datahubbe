from dataclasses import fields
from rest_framework import serializers
from datahub_v3_app.models import sql_generator


class SQL_Generator_Serializer(serializers.ModelSerializer):

    class Meta:
        model= sql_generator
        fields = '__all__'