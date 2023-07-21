from rest_framework import serializers
from datahub_v3_app.models import ScheduleDependency

class Schedule_Dependency_Serializer(serializers.ModelSerializer):
    class Meta:
        model = ScheduleDependency
        fields =  '__all__'

    # def create(self, validated_data):
    #     return ScheduleDependency.objects.create(validated_data)