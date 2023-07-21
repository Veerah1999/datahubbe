from rest_framework import serializers
from datahub_v3_app.models import users_role_view

class Users_Role_Serializer(serializers.ModelSerializer):

    class Meta:
        model= users_role_view
        fields= ['id','user_name','role_name','start_date','end_date','is_active']