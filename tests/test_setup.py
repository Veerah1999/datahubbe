import imp
from rest_framework.test import APITestCase, APIClient
from django.urls import reverse


class Testsetup(APITestCase):
   
   def setUp(self):
      self.register_url = reverse('register')
      self.login_url = reverse('login')

      user_data = {
      "first_name": "admin",
      "last_name": "A",
      "email": "admin@gmail.com",
      "phone_number": "9940748470",
      "password":"admin123",
      "alternate_phonenumber": "807228405",
      "addressline_one": "Chennai",
      "addressline_two": "TN",
      "countryor_city": "India",
      "postalcode": "639006",
      "company_name": "DM",
      "company_type": "SW",
      "category":"company"
      }
      
      return super().setUp()

   def  tearDown(self):
       return super().tearDown()