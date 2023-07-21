from datahub_v3_app.models import role_api
from rest_framework import serializers


class Role_Serializer(serializers.ModelSerializer):

    class Meta:
        model= role_api
        fields= ['id','role_name','role_desc','role_handling_pages','role_start_date','role_end_date','role_status']
