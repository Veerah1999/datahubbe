from django.http import JsonResponse
from .models import User
import jwt
from django.conf import settings
from rest_framework.exceptions import AuthenticationFailed
import datetime
from jwt.exceptions import ExpiredSignatureError
class BearerTokenAuthenticationMiddleware:
    """
    Middleware for API authentication using a bearer token.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Check if the request is for an API endpoint
        url = ('/connection_det/')
        if request.path.startswith(url):
            # Get the bearer token from the request headers
            auth_header = request.headers.get('Authorization')
            if not auth_header:
                # Return a 401 Unauthorized response if no token is provided
                return JsonResponse({'error': 'Unauthorized'}, status=401)

            try:
                auth_type, token = auth_header.split()
            except ValueError:
                # Return a 401 Unauthorized response if the Authorization header is invalid
                return JsonResponse({'error': 'Invalid Authorization header'}, status=401)

            if auth_type.lower() != 'bearer':
                # Return a 401 Unauthorized response if the Authorization type is not Bearer
                return JsonResponse({'error': 'Invalid Authorization type'}, status=401)
            
        #     expiration_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=1)

        #     payload = {
        #     'id': User.id,
        #     'email': User.email,
        #     'exp': expiration_time,
        #     'iat': datetime.datetime.utcnow()
        # }
        #     token = jwt.encode(payload, 'secret', algorithm='HS256')
        #     if expiration_time < datetime.datetime.utcnow():
        #      raise AuthenticationFailed('Token has expired')

            # Verify the token
            try:
                decoded_token = jwt.decode(token, 'secret', algorithms=['HS256'])
                user_id = decoded_token.get('id')
                user = User.objects.get(id=user_id)
                request.user = user
                print('hi')
            except ExpiredSignatureError:
                 return JsonResponse({'error': 'Token has expired'}, status=401)
            except (jwt.exceptions.DecodeError, User.DoesNotExist):
                # Return a 401 Unauthorized response if the token is invalid
                return JsonResponse({'error': 'Invalid token'}, status=401)

        response = self.get_response(request)

        return response


from django.utils.deprecation import MiddlewareMixin

class DisableCSRFMiddleware(MiddlewareMixin):
    def process_request(self, request):
        setattr(request, '_dont_enforce_csrf_checks', True)
