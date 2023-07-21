from datetime import date
from datahub_v3_app.models import pipeline

class DeactivateExpiredConnectionsMiddleware:
    def _init_(self, get_response):
        self.get_response = get_response

    def _call_(self, request):
        now = date.today()
        expired_connections = pipeline.objects.filter(end_date__lte=now, is_active=True)
        for connection in expired_connections:
            connection.is_active = False
            connection.save()
        response = self.get_response(request)
        return response
