

import os
from pathlib import Path
from datetime import timedelta
# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('DJANGO_SECRET_KEY', 'django-insecure-j%e^1mov931g1ux4c_cz*a^qdcve)iy-a(s@$l=v-@n$h=t+!3')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get('DEBUG', 'True') == 'True'

ALLOWED_HOSTS = os.environ.get('ALLOWED_HOSTS', 'localhost,127.0.0.1,django-app').split(',')


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    
    # Third party apps
    'rest_framework',
    'corsheaders',
    'drf_yasg',
    'drf_spectacular',  # Add this
    'drf_spectacular_sidecar',  # Optional - for offline assets
    # Your apps
    'scheduler_manager',
]

# drf-spectacular settings
SPECTACULAR_SETTINGS = {
    'TITLE': 'SMS Campaign Manager API',
    'DESCRIPTION': '''
    Comprehensive API for managing SMS campaigns with multi-language support.
    
    ## Features
    - Campaign CRUD operations
    - Multi-language message templates (EN, AM, TI, OM, SO)
    - Audience management with recipient validation
    - Campaign scheduling with time windows
    - Progress tracking and monitoring
    - Batch processing status
    - Message delivery status tracking
    - JWT Authentication
    
    ## Authentication
    All endpoints except user registration require JWT authentication.
    Obtain token via `/api/token/` endpoint.
    ''',
    'VERSION': '1.0.0',
    'SERVE_INCLUDE_SCHEMA': False,
    'COMPONENT_SPLIT_REQUEST': True,
    'SWAGGER_UI_SETTINGS': {
        'deepLinking': True,
        'persistAuthorization': True,
        'displayOperationId': True,
        'docExpansion': 'list',
        'filter': True,
    },
    'TAGS': [
        {'name': 'Campaigns', 'description': 'Complete campaign management (CRUD + actions)'},
        {'name': 'Campaign - Schedule', 'description': 'Campaign schedule management'},
        {'name': 'Campaign - Message Content', 'description': 'Multi-language message template management'},
        {'name': 'Campaign - Audience', 'description': 'Campaign recipient management'},
        {'name': 'Campaign - Progress', 'description': 'Campaign progress and monitoring'},
        {'name': 'Campaign - Actions', 'description': 'Campaign control actions (start/stop/complete)'},
        {'name': 'Schedules', 'description': 'View all campaign schedules'},
        {'name': 'Message Status', 'description': 'Message delivery status tracking'},
        {'name': 'Campaign Progress', 'description': 'Campaign progress overview'},
        {'name': 'Batches', 'description': 'Message batch processing status'},
        {'name': 'Users', 'description': 'User management and registration'},
        {'name': 'Authentication', 'description': 'JWT authentication endpoints'},
    ],
    'ENUM_NAME_OVERRIDES': {
        'CampaignStatusEnum': 'scheduler_manager.models.Campaign.STATUS_CHOICES',
        'ChannelEnum': 'scheduler_manager.models.Campaign.CHANNEL_CHOICES',
        'MessageStatusEnum': 'scheduler_manager.models.MessageStatus.MESSAGE_STATUS_CHOICES',
        'BatchStatusEnum': 'scheduler_manager.models.BatchStatus.BATCH_STATUS_CHOICES',
    },
}

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'myproject.urls'

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

WSGI_APPLICATION = 'myproject.wsgi.application'


# Database
# https://docs.djangoproject.com/en/5.2/ref/settings/#databases

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.postgresql',
#         'NAME': 'campaign_db',  # Should be campaign_db, not campaign_user
#         'USER': 'campaign_user',
#         'PASSWORD': 'campaign_pass',
#         'HOST': 'campaign-db',
#         'PORT': '5432',
#     }
# }

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.postgresql',
#         'NAME': 'postgres',  # Should be campaign_db, not campaign_user
#         'USER': 'postgres',
#         'PASSWORD': 'Efrem12.',
#         'HOST': 'localhost',
#         'PORT': '5432',
#     }
# }

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
        # Optional: Add these for better performance
        # 'OPTIONS': {
        #     'timeout': 20,  # Increase timeout for busy database
        #     'transaction_mode': 'IMMEDIATE',  # Better concurrency
        # },
    }
}

# Password validation
# https://docs.djangoproject.com/en/5.2/ref/settings/#auth-password-validators

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
# https://docs.djangoproject.com/en/5.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.2/howto/static-files/

STATIC_URL = 'static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')

# Default primary key field type
# https://docs.djangoproject.com/en/5.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# CORS settings
CORS_ALLOWED_ORIGINS = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://django-app:8000",
    "http://172.27.131.128:8080",  # Your WSL IP
]

CORS_ALLOW_ALL_ORIGINS = True  # For development only - remove in production

# REST Framework settings
REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework_simplejwt.authentication.JWTAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
         'rest_framework.permissions.IsAuthenticated',  # Change for production
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 100,
    
}

SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(minutes=60),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=1),
    'ROTATE_REFRESH_TOKENS': False,
    'BLACKLIST_AFTER_ROTATION': True,
}
# Swagger settings
SWAGGER_SETTINGS = {
    'SECURITY_DEFINITIONS': None,
    'USE_SESSION_AUTH': False,
    'JSON_EDITOR': True,
    'SUPPORTED_SUBMIT_METHODS': [
        'get',
        'post',
        'put',
        'delete',
        'patch'
    ],
}

# Campaign Manager specific settings
SCHEDULER_MANAGER_ENABLED = os.environ.get('SCHEDULER_MANAGER_ENABLED', 'True') == 'True'
SMS_SERVICE_ENABLED = os.environ.get('SMS_SERVICE_ENABLED', 'False') == 'False'