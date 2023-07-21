from rest_framework import serializers

class FlattenFileSerializer(serializers.Serializer):
    input_path = serializers.CharField(max_length=255)
    output_path = serializers.CharField(max_length=255)
