from dataclasses import fields
from rest_framework import serializers
from datahub_v3_app.models import *

class Member_Serializer(serializers.ModelSerializer):
    class Meta:
        model= member
        fields = ['member_name','team_id_id','tenant_id_id','mail_id','id']


class Team_Serializer(serializers.ModelSerializer):
    class Meta:
        model= teams_api_model
        fields = ['team_name','role_handling_pages','tenant_id_id','id']
