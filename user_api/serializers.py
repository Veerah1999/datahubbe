from rest_framework import serializers


from datahub_v3_app.models import user_api



class user_serializer(serializers.ModelSerializer):

    class Meta:
        model= user_api
        fields= '__all__'
