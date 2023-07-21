from django.contrib.auth.backends import BaseBackend
from datahub_v3_app.models import tenant_user

class CustomAuthBackend(BaseBackend):
    def authenticate(self, request, email=None, password=None):
        try:
            user = tenant_user.objects.get(email=email)
        except tenant_user.DoesNotExist:
            return None
        
        if user.check_password(password):
            return user
        
        return None
    
    def get_user(self, user_id):
        try:
            return tenant_user.objects.get(pk=user_id)
        except tenant_user.DoesNotExist:
            return None

