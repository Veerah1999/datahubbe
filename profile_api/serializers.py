from dataclasses import dataclass
import email
import imp
from urllib import request
from rest_framework import serializers
from datahub_v3_app.models import User
from django.forms import ValidationError
import logging
import json


class Profile_Serializer(serializers.ModelSerializer):
    

    class Meta:
        model = User
        fields = ['id','first_name','last_name','email','phone_number','alternate_phonenumber',
                  'addressline_one','addressline_two','countryor_city','postalcode', 'company_name','company_type',
                  'category']
