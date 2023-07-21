from rest_framework import serializers
from datahub_v3_app.models import db_sql_table

class dbextract_Serializer(serializers.ModelSerializer):

    class Meta:
        model =  db_sql_table
        fields = ['id','database_name','sequelize_query','sql_validation','sql_status','start_date','end_date','is_active']
