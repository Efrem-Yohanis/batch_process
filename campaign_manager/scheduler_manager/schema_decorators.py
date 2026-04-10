# scheduler_manager/schema_decorators.py
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample

# Campaign decorators
campaign_list_schema = extend_schema(
    tags=['Campaigns'],
    summary='List all campaigns',
    description='Retrieve a paginated list of all campaigns with optional filtering.',
    parameters=[
        OpenApiParameter(name='status', type=str, location='query', 
                        description='Filter by campaign status', 
                        enum=['draft', 'active', 'paused', 'completed', 'archived']),
        OpenApiParameter(name='execution_status', type=str, location='query',
                        description='Filter by execution status',
                        enum=['PENDING', 'PROCESSING', 'PAUSED', 'STOPPED', 'COMPLETED', 'FAILED']),
        OpenApiParameter(name='channel', type=str, location='query',
                        description='Filter by channel',
                        enum=['sms', 'app_notification', 'flash_sms']),
    ],
)

campaign_start_schema = extend_schema(
    tags=['Campaigns - Actions'],
    summary='Start a campaign',
    description='Start a campaign. Validates that all required components are present.',
    request=None,
    examples=[
        OpenApiExample(
            'Success Response',
            value={
                'status': 'active',
                'execution_status': 'PROCESSING',
                'message': 'Campaign is now active',
                'campaign_id': 1,
                'total_recipients': 1000
            },
            response_only=True,
        )
    ],
)

# Audience decorators
audience_create_schema = extend_schema(
    tags=['Audiences'],
    summary='Create audience',
    description='Upload recipient list for a campaign. Phone numbers must be in E.164 format.',
    examples=[
        OpenApiExample(
            'Example Request',
            value={
                'campaign': 1,
                'recipients': [
                    {'msisdn': '+251912345678', 'lang': 'en'},
                    {'msisdn': '+251911223344', 'lang': 'am'},
                ]
            },
            request_only=True,
        ),
    ],
)

# Message Content decorators
message_content_create_schema = extend_schema(
    tags=['Message Contents'],
    summary='Create message content',
    description='Create multi-language message template for a campaign.',
    examples=[
        OpenApiExample(
            'Multi-language Example',
            value={
                'campaign': 1,
                'content': {
                    'en': 'Hello {name}!',
                    'am': 'ሰላም {name}!',
                    'ti': 'ሰላም {name}!',
                    'om': 'Akkam {name}!',
                    'so': 'Salaan {name}!'
                },
                'default_language': 'en'
            },
            request_only=True,
        ),
    ],
)

# Schedule decorators
schedule_create_schema = extend_schema(
    tags=['Schedules'],
    summary='Create schedule',
    description='Create a schedule for a campaign. Supports multiple schedule types.',
    examples=[
        OpenApiExample(
            'Daily Schedule',
            value={
                'campaign': 1,
                'schedule_type': 'daily',
                'start_date': '2024-01-01',
                'time_windows': [{'start': '09:00', 'end': '17:00'}],
                'timezone': 'Africa/Addis_Ababa'
            },
        ),
        OpenApiExample(
            'Weekly Schedule',
            value={
                'campaign': 1,
                'schedule_type': 'weekly',
                'start_date': '2024-01-01',
                'run_days': [0, 2, 4],  # Mon, Wed, Fri
                'time_windows': [{'start': '10:00', 'end': '12:00'}],
            },
        ),
    ],
)