
# from dataclasses import dataclass
# import email
# from rest_framework import serializers
# from .models import User
# from django.forms import ValidationError



# class UserSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = User
#         fields = ['id', 'first_name', 'last_name', 'email','phone_number','password','confirm_password','alternate_phonenumber','addressline_one',
#         'addressline_two','countryor_city','postalcode','company_name','company_type']
#         extra_kwargs = {
#             'password': {'write_only': True},
#             'confirm_password': {'write_only': True}
#         }
        
#     def create(self, validated_data):
#         password = validated_data.pop('password', None)
#         instance = self.Meta.model(**validated_data)
#         if password is not None:
#             instance.set_password(password)
#         instance.save()
#         return instance

#     def validate(self,data):
#         if data['password'] != data['confirm_password']:
#             raise serializers.ValidationError("Password mismatched")
#         return data

#     def validate(self, attrs):
#         email_exists = User.objects.filter(email=attrs["email"]).exists()

#         if email_exists:
#             raise ValidationError("Email has already been used")
#         return super().validate(attrs)

# class LoginSerializer(serializers.Serializer):
#     email = serializers.CharField(max_length=50, required=True)
#     password = serializers.CharField(max_length=2000, required=True)

#     class Meta:
#         fields = ['password','email']

