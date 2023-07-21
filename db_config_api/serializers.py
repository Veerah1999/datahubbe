from rest_framework import serializers
from datahub_v3_app.models import db_config


class Db_Config_Serializer(serializers.ModelSerializer):

    class Meta:
        model= db_config
        fields= ['id','config_name','desc','source_connection_name','target_connection_name','start_date','end_date','is_active',
        'Source_conn_det_id','Target_conn_det_id']