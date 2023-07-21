from rest_framework import serializers


from datahub_v3_app.models import role_api



class NewCount_Serializer(serializers.ModelSerializer):

    class Meta:
        model= role_api
        fields= '_all_'