from rest_framework import serializers
from datahub_v3_app.models import tenant_user


class Tenant_User_Serializer(serializers.ModelSerializer):
     
    email=serializers.CharField(required=True),
    phone_number=serializers.CharField(required=True),
    password=serializers.CharField(required=True),
    address=serializers.CharField(required=True),
    country=serializers.CharField(required=True),
    city=serializers.CharField(required=True),
    postalcode=serializers.CharField(required=True)
    company_name=serializers.CharField(required=True),
    company_reg_no=serializers.CharField(required=True),
    company_pan_no=serializers.CharField(required=True), 
    tenant_id=serializers.CharField(required=True),
    role=serializers.CharField(required=True)

    class Meta:
        model = tenant_user
        fields = '__all__'
        extra_kwargs = {
            'password': {'write_only': True}    
        }
        
    def create(self, validated_data):
        password = validated_data.pop('password', None)
        instance = self.Meta.model(**validated_data)
        if password is not None:
            instance.set_password(password)
        instance.save()
        return instance
