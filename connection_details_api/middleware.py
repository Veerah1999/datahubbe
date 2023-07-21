from datetime import date
from datahub_v3_app.models import connection_detail

class DeactivateExpiredConnectionsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        now = date.today()
        expired_connections = connection_detail.objects.filter(end_date__lte=now, is_active=True)
        for connection in expired_connections:
            connection.is_active = False
            connection.save()
        response = self.get_response(request)
        return response
