from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging

API_URL = "http://django-app:8000/api"
SMS_LAYER1_URL = "http://sms-layer1:8001"  # Layer 1 API endpoint

default_args = {
    'owner': 'sms_scheduler',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'campaign_scheduler',
    default_args=default_args,
    description='Check schedules and trigger campaigns via SMS Layer 1',
    schedule_interval='* * * * *',  # Every minute
    catchup=False,
    tags=['sms'],
)

def check_schedules(**context):
    """Check all schedules and trigger campaigns via Layer 1 API"""
    logger = logging.getLogger('airflow.task')
    now = datetime.now()
    
    logger.info(f"=== Checking schedules at {now} ===")
    
    try:
        # 1. Get schedules from Django
        response = requests.get(f"{API_URL}/schedules/", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            schedules = data.get('results', [])
            logger.info(f"✅ Found {len(schedules)} schedules")
            
            for schedule in schedules:
                campaign_id = schedule['campaign']
                status = schedule['status']
                send_times = schedule.get('send_times', [])
                
                logger.info(f"  Schedule {schedule['id']}: Campaign {campaign_id} | Status: {status} | Send times: {send_times}")
                
                # 2. Check if it's time to start
                current_time = now.strftime("%H:%M")
                if current_time in send_times and status == 'pending':
                    logger.info(f"🚀 Time to start campaign {campaign_id}!")
                    
                    # 3. Update campaign status in Django
                    try:
                        start_response = requests.post(
                            f"{API_URL}/campaigns/{campaign_id}/start/",
                            json={'triggered_by': 'scheduler'},
                            timeout=5
                        )
                        if start_response.status_code == 200:
                            logger.info(f"✅ Django: Campaign {campaign_id} marked as started")
                            
                            # 4. Call SMS Layer 1 to start sending messages
                            sms_response = requests.post(
                                f"{SMS_LAYER1_URL}/campaign/start",
                                json={"campaign_id": campaign_id, "triggered_by": "airflow"},
                                timeout=5
                            )
                            if sms_response.status_code == 202:
                                logger.info(f"✅ SMS Layer 1: Campaign {campaign_id} queued for sending")
                                logger.info(f"   Response: {sms_response.json()}")
                            else:
                                logger.error(f"❌ SMS Layer 1 returned {sms_response.status_code}")
                    except Exception as e:
                        logger.error(f"❌ Failed to start campaign {campaign_id}: {e}")
                
                # 5. Check if it's time to stop
                end_times = schedule.get('end_times', [])
                if current_time in end_times and status == 'running':
                    logger.info(f"🛑 Time to stop campaign {campaign_id}!")
                    
                    # 6. Update campaign status in Django
                    try:
                        stop_response = requests.post(
                            f"{API_URL}/campaigns/{campaign_id}/stop/",
                            json={'triggered_by': 'scheduler'},
                            timeout=5
                        )
                        if stop_response.status_code == 200:
                            logger.info(f"✅ Django: Campaign {campaign_id} marked as stopped")
                            
                            # 7. Call SMS Layer 1 to stop sending
                            sms_response = requests.post(
                                f"{SMS_LAYER1_URL}/campaign/stop",
                                json={"campaign_id": campaign_id, "triggered_by": "airflow"},
                                timeout=5
                            )
                            if sms_response.status_code == 202:
                                logger.info(f"✅ SMS Layer 1: Campaign {campaign_id} stopped")
                            else:
                                logger.error(f"❌ SMS Layer 1 returned {sms_response.status_code}")
                    except Exception as e:
                        logger.error(f"❌ Failed to stop campaign {campaign_id}: {e}")
        else:
            logger.error(f"❌ Django API returned {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        logger.error(f"❌ Cannot connect to Django API at {API_URL}")
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")

check_task = PythonOperator(
    task_id='check_schedules',
    python_callable=check_schedules,
    dag=dag,
)