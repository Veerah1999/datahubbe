from rest_framework import serializers
from datahub_v3_app.models import datatype


class Datatype_Serializer(serializers.ModelSerializer):

    class Meta:
        model= datatype
        fields= ['id','config_id','config_name','source_id','source_name','target_id','target_name','datatype_mapping_name','datatype']