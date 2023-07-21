from dataclasses import fields
from rest_framework import serializers
from datahub_v3_app.models import pages


class Page_Serializer(serializers.ModelSerializer):

    class Meta:
        model= pages
        fields= ['id','page_name','module_name','page_desc','start_date','end_date','is_active','created_by','created_on']
