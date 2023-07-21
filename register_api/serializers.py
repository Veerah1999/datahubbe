from dataclasses import dataclass
import email
import imp
from urllib import request
from rest_framework import serializers
from datahub_v3_app.models import User
from django.forms import ValidationError
import logging
import json


class User_Serializer(serializers.ModelSerializer):
    first_name=serializers.CharField(required=True),
    last_name = serializers.CharField(required=False),
    email=serializers.CharField(required=True),
    phone_number=serializers.CharField(required=True),
    password=serializers.CharField(required=True),
    alternate_phonenumber=serializers.CharField(required=False),
    addressline_one=serializers.CharField(required=True),
    addressline_two=serializers.CharField(required=False),
    countryor_city=serializers.CharField(required=True),
    postalcode=serializers.CharField(required=True),
    company_name=serializers.CharField(required=False),
    company_type=serializers.CharField(required=False),
    category=serializers.CharField(required=True)

    class Meta:
        model = User
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

    # def validate(self,data):
    #     if data['password'] != data['confirm_password']:
    #         raise serializers.ValidationError(Password mismatched)
    #     return data


# from dataclasses import fields
# from rest_framework import serializers
# from datahub_v3_app.models import User
# from django.db import models
# from rest_framework.response import Response



# class UserSerializer(serializers.Serializer):
#     first_name = serializers.CharField(max_length=255)
#     last_name = serializers.CharField(max_length=255)
#     email = serializers.CharField(max_length=255)
#     phone_number = serializers.CharField(max_length=15)
#     password = serializers.CharField(max_length=255)
#     alternate_phonenumber = serializers.CharField(max_length=15)
#     addressline_one = serializers.CharField(max_length=100)
#     addressline_two = serializers.CharField(max_length=100)
#     countryor_city = serializers.CharField(max_length=100)
#     postalcode = serializers.CharField(max_length=100)
#     company_name = serializers.CharField(max_length=100)
#     company_type = serializers.CharField(max_length=100)
#     category = serializers.CharField(max_length=100)
#     last_name = serializers.CharField(max_length=255,required=False)
#     alternate_phonenumber = serializers.CharField(max_length=15,required=False)
#     addressline_two = serializers.CharField(max_length=100,required=False)
#     company_name = serializers.CharField(max_length=100,required=False)
#     company_type = serializers.CharField(max_length=100,required=False)
    
#     class Meta:
#         model = User
#         fields = '__all__'
#         extra_kwargs = {
#             'password': {'write_only': True}    
#         }

#     # def get_validation_exclusions(self):
#     #     exclusions = super(UserSerializer, self).get_validation_exclusions()
#     #     return exclusions + ['last_name','alternate_phonenumber','addressline_two','company_name','company_type']

#     def create(self, validated_data):
#         password = validated_data.pop('password', None)
#         instance = self.Meta.model(**validated_data)
#         if password is not None:
#             instance.set_password(password)
#         instance.save()
#         return instance

#     # def validate(self,data):
#     #     if data['password'] != data['confirm_password']:
#     #         raise serializers.ValidationError("Password mismatched")
#     #     return data