from rest_framework import serializers


from datahub_v3_app.models import schema_migration



class Schema_Serializer(serializers.ModelSerializer):

    class Meta:
        model= schema_migration
        fields= '__all__'