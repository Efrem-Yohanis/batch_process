import os
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

# Build paths
BASE_DIR = Path(__file__).resolve().parent.parent

# =============== SECURITY ===============
SECRET_KEY = os.environ.get('DJANGO_SECRET_KEY', 'django-insecure-your-key-here-change-in-production')
DEBUG = os.environ.get('DEBUG', 'True') == 'True'
ALLOWED_HOSTS = ['localhost', '127.0.0.1', 'django-app', 'campaign-db', '0.0.0.0']

# =============== APPLICATION DEFINITION ===============
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'rest_framework_simplejwt',
    'corsheaders',
    'drf_spectacular',
    'drf_spectacular_sidecar',
    'scheduler_manager',
]

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

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'campaign_db',
        'USER': 'campaign_user',
        'PASSWORD': 'campaign_pass',
        'HOST': 'campaign-db',
        'PORT': '5432',
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

STATIC_URL = 'static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

CORS_ALLOW_ALL_ORIGINS = True

REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': ('rest_framework_simplejwt.authentication.JWTAuthentication',),
    'DEFAULT_PERMISSION_CLASSES': ('rest_framework.permissions.IsAuthenticated',),
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 100,
}

SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(minutes=60),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=1),
    'ROTATE_REFRESH_TOKENS': False,
    'BLACKLIST_AFTER_ROTATION': True,
    'UPDATE_LAST_LOGIN': True,
    'ALGORITHM': 'HS256',
    'SIGNING_KEY': SECRET_KEY,
    'AUTH_HEADER_TYPES': ('Bearer',),
    'USER_ID_FIELD': 'id',
    'USER_ID_CLAIM': 'user_id',
}

# =============== SPECTACULAR (SWAGGER) SETTINGS ===============
SPECTACULAR_SETTINGS = {
    'TITLE': 'Campaign Manager API',
    'DESCRIPTION': '''
    # Campaign Management System API
    
    API for managing SMS campaigns with multi-language support, scheduling, and audience management.
    
    ## Authentication
    All endpoints (except health check) require JWT authentication.
    
    **To authenticate:**
    1. Call `POST /api/token/` with your username and password
    2. Copy the `access` token from the response
    3. Click the "Authorize" button and enter: `Bearer <your-access-token>`
    
    ## API Categories
    
    ### 📱 Campaign Management
    Complete CRUD operations for campaigns plus lifecycle actions (start, pause, resume, stop, complete)
    
    ### ⏰ Schedule Management
    Configure one-time or recurring schedules (daily, weekly, monthly) with multiple time windows
    
    ### 👥 Audience Management
    Upload and manage recipient lists with phone number validation and language support
    
    ### 📝 Message Content Management
    Multi-language message templates with variable substitution support
    
    ### 📊 Message Status Tracking
    Track message delivery status and provider responses
    
    ### 📈 Progress Monitoring
    Monitor campaign execution progress and batch status
    
    ### 👤 User Management
    User registration and profile management
    ''',
    'VERSION': '1.0.0',
    'SERVE_INCLUDE_SCHEMA': False,
    'SWAGGER_UI_SETTINGS': {
        'deepLinking': True,
        'persistAuthorization': True,
        'displayOperationId': True,
        'displayRequestDuration': True,
        'filter': True,
        'tryItOutEnabled': True,
        'syntaxHighlight': {
            'activated': True,
            'theme': 'monokai'
        },
        'layout': 'BaseLayout',
        'docExpansion': 'list',
        'defaultModelsExpandDepth': 3,
        'defaultModelExpandDepth': 3,
    },
    'SWAGGER_UI_DIST': 'SIDECAR',
    'SWAGGER_UI_FAVICON_HREF': 'SIDECAR',
    'REDOC_DIST': 'SIDECAR',
    'TAGS': [
        # Campaign Management
        {
            'name': 'Campaigns',
            'description': '📱 **Campaign Management** - Create, read, update, and delete campaigns'
        },
        {
            'name': 'Campaigns - Actions',
            'description': '▶️ **Campaign Lifecycle** - Start, pause, resume, stop, complete, and archive campaigns'
        },
        {
            'name': 'Campaigns - Progress',
            'description': '📈 **Campaign Progress** - View progress, statistics, and batches'
        },
        
        # Schedule Management
        {
            'name': 'Schedules',
            'description': '⏰ **Schedule Management** - Configure one-time or recurring schedules (daily, weekly, monthly)'
        },
        
        # Audience Management
        {
            'name': 'Audiences',
            'description': '👥 **Audience Management** - Upload and manage recipient lists with validation'
        },
        
        # Message Content
        {
            'name': 'Message Contents',
            'description': '📝 **Message Templates** - Multi-language content with variable substitution'
        },
        
        # Message Status
        {
            'name': 'Message Statuses',
            'description': '📊 **Delivery Tracking** - Track message status and provider responses'
        },
        
        # Progress Monitoring
        {
            'name': 'Campaign Progress',
            'description': '📈 **Execution Monitoring** - Track campaign execution progress'
        },
        
        # Checkpoints
        {
            'name': 'Checkpoints',
            'description': '🔍 **Processing Checkpoints** - Resumable campaign execution tracking'
        },
        
        # Batches
        {
            'name': 'Batches',
            'description': '📦 **Batch Management** - Track message batches'
        },
        
        # User Management
        {
            'name': 'Users',
            'description': '👤 **User Management** - Registration, profile, and password management'
        },
        
        # Authentication
        {
            'name': 'Authentication',
            'description': '🔐 **JWT Authentication** - Obtain and refresh access tokens'
        },
        
        # System
        {
            'name': 'System',
            'description': '🏥 **System Health** - Health checks and system information'
        },
    ],
    'COMPONENT_SPLIT_REQUEST': True,
    'SORT_OPERATIONS': False,
    'ENABLE_DJANGO_DEPLOY_CHECK': False,
    'SCHEMA_PATH_PREFIX': '/api/',
}

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {'format': '{levelname} {asctime} {message}', 'style': '{'},
    },
    'handlers': {
        'console': {'class': 'logging.StreamHandler', 'formatter': 'simple'},
    },
    'root': {'handlers': ['console'], 'level': 'INFO'},
}