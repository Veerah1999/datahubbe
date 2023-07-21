"""
Django settings for datahub_v3_project project.

Generated by 'django-admin startproject' using Django 3.2.10.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.2/ref/settings/
"""

# from imghdr import tests
from pathlib import Path
from celery.schedules import crontab
import datetime
import os
# from django.utils.encoding import force_str
# import django
# from django.utils.encoding import force_str
# django.utils.encoding.force_text = force_str

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-or2gd*n%7g4auwp4h_*fg8j#-=rujvnjqcq2&3xtsl=d6#jj4i'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['*']


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'corsheaders',
    'drf_yasg',
    'django_filters',
    'datahub_v3_app',
    'register_api',
    'login_api',
    'connection_api',
    # 'schedule_dependency'
    # 'db_sql_extract',
    'pipeline_api',
    'pipeline_details_api',
    'connection_details_api',
    'db_config_api',
    'role_detail_api',
    'profile_api',
    'user_role_api',
    'user_api',
    'tenant_api',
    'rest_framework.authtoken',
    'rest_framework_jwt',
    ]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'connection_api.middleware.DeactivateExpiredConnectionsMiddleware',
    'connection_details_api.middleware.DeactivateExpiredConnectionsMiddleware',
    'db_config_api.middleware.DeactivateExpiredConnectionsMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.security.SecurityMiddleware',        
   # 'datahub_v3_app.middleware.BearerTokenAuthenticationMiddleware',
    'datahub_v3_app.middleware.DisableCSRFMiddleware', 
    # 'datahub_v3_app.utils.DisableCSRF', 

]


REST_FRAMEWORK = {
    'DEFAULT_FILTER_BACKENDS': ['django_filters.rest_framework.DjangoFilterBackend'],
       'DEFAULT_AUTHENTICATION_CLASSES': (
       'rest_framework.authentication.TokenAuthentication',
   ),
   'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticate',
   ],
   'DEFAULT_AUTHENTICATION_CLASSES': [
    'rest_framework.authentication.SessionAuthentication',
    'rest_framework.authentication.BasicAuthentication',
    'rest_framework_simplejwt.authentication.JWTAuthentication',
    
],
    # 'DEFAULT_AUTHENTICATION_CLASSES': ( 'rest_framework_jwt.authentication.JSONWebTokenAuthentication', ),
}           #new

JWT_AUTH = {
     'JWT_SECRET_KEY': 'django-insecure-or2gd*n%7g4auwp4h_*fg8j#-=rujvnjqcq2&3xtsl=d6#jj4i',
     'JWT_ALGORITHM': 'HS256',
     'JWT_ALLOW_REFRESH': True,
     'JWT_EXPIRATION_DELTA': datetime.timedelta(days=1),
     'JWT_REFRESH_EXPIRATION_DELTA': datetime.timedelta(days=7),
       }


CORS_ORIGIN_ALLOW_ALL = True

CORS_ALLOW_METHODS = [
    'DELETE',
    'GET',
    'OPTIONS',
    'PATCH',
    'POST',
    'PUT',
]

CORS_ALLOW_HEADERS = [
    'accept',
    'accept-encoding',
    'authorization',
    'content-type',
    'dnt',
    'origin',
    'user-agent',
    'x-csrftoken',
    'x-requested-with',
    'token'
]   #new

CORS_EXPOSE_HEADERS = CORS_ALLOW_HEADERS  #new

ROOT_URLCONF = 'datahub_v3_project.urls'

REST_FRAMEWORK={
    'NON_FIELD_ERRORS_KEY': 'errors',

}

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'datahub_v3_project.wsgi.application'


# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': 'decihub123',
        'HOST':'18.217.196.203',
       'PORT': '5432',

    }
}



# Password validation
# https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/

STATIC_URL = '/static/'
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')
MEDIA_URL = '/media/'  #new
# Default primary key field type
# https://docs.djangoproject.com/en/3.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
AUTH_USER_MODEL = 'datahub_v3_app.User'


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
}

from celery.schedules import crontab
from datahub_v3_project import task


CELERY_BROKER_URL = "redis://localhost:6379"
CELERY_RESULT_BACKEND = "redis://localhost:6379"


CELERY_BEAT_SCHEDULE = {
    "sample_task": {
        "task": "datahub_v3_project.tasks.sample_task",
        "schedule": crontab(minute="*/1"),
    },
}

# CSRF_TRUSTED_ORIGINS = [
#     'https://datahub_v3_app.herokuapp.com'
# ]

# JWT_AUTH = {
#     # Authorization:Token xxx
#     'JWT_AUTH_HEADER_PREFIX': 'Token',
# }
from datetime import timedelta
SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(minutes=60),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=1),
    'ROTATE_REFRESH_TOKENS': False,
    'BLACKLIST_AFTER_ROTATION': False,
    'UPDATE_LAST_LOGIN': False,
    'ALGORITHM': 'HS256',
    'SIGNING_KEY': SECRET_KEY,
    'VERIFYING_KEY': None,
    'AUDIENCE': None,
    'ISSUER': None,
    'JWK_URL': None,
    'LEEWAY': 0,    
    'AUTH_HEADER_TYPES': ('Bearer',),
    'AUTH_HEADER_NAME': 'HTTP_AUTHORIZATION',
    'USER_ID_FIELD': 'id',
    'USER_ID_CLAIM': 'user_id',
    'USER_AUTHENTICATION_RULE': 'rest_framework_simplejwt.authentication.default_user_authentication_rule',
    'AUTH_TOKEN_CLASSES': ('rest_framework_simplejwt.tokens.AccessToken',),
    'TOKEN_TYPE_CLAIM': 'token_type',
    'TOKEN_USER_CLASS': 'rest_framework_simplejwt.models.TokenUser',
    'JTI_CLAIM': 'jti',
    'SLIDING_TOKEN_REFRESH_EXP_CLAIM': 'refresh_exp',
    'SLIDING_TOKEN_LIFETIME': timedelta(minutes=60),
    'SLIDING_TOKEN_REFRESH_LIFETIME': timedelta(days=1),

}


SIMPLE_JWT = {
'ACCESS_TOKEN_LIFETIME': datetime.timedelta(seconds=60),
'REFRESH_TOKEN_LIFETIME': datetime.timedelta(minutes=2),
}

AUTHENTICATION_BACKENDS = [
    'datahub_v3_app.backend.CustomAuthBackend',
    'django.contrib.auth.backends.ModelBackend',
]
