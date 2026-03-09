Access Points
Service	URL	Credentials
Django Admin	http://localhost:8000/admin	     test / test
Django API	http://localhost:8000/api	-
Layer 1 API	http://localhost:8001/docs	-
Layer 3 Webhook	http://localhost:8002/webhook/delivery	-
Layer 3 Health	http://localhost:8002/health	-
Airflow UI	http://localhost:8080	test / test
Kafka UI	http://localhost:8082	-
Redis Commander	http://localhost:8083	-
 
 
 
✔ Network batch_process_scheduler-network        Created                                                                                                0.2s
✔ Volume "batch_process_django-static"           Created                                                                                                0.0s
✔ Volume "batch_process_campaign-postgres-data"  Created                                                                                                0.0s
✔ Volume "batch_process_redis-data"              Created                                                                                                0.0s
✔ Volume "batch_process_airflow-postgres-data"   Created                                                                                                0.0s
✔ Container airflow-postgres                     Healthy                                                                                                0.1s
✔ Container redis-dispatch                       Healthy                                                                                                0.1s
✔ Container campaign-postgres                    Healthy                                                                                                0.1s
✔ Container zookeeper                            Healthy                                                                                                0.2s
✔ Container django-campaign-manager              Started                                                                                                0.1s
✔ Container airflow-init                         Exited                                                                                                 0.2s
✔ Container redis-commander                      Started                                                                                                0.2s
✔ Container kafka                                Healthy                                                                                                0.1s
✔ Container kafka-ui                             Started                                                                                                0.1s
✔ Container airflow-scheduler                    Started                                                                                                0.2s
✔ Container sms-layer2                           Started                                                                                                0.2s
✔ Container sms-layer1                           Started                                                                                                0.2s
✔ Container sms-layer3                           Started                                                                                                0.2s
✔ Container airflow-webserver                    Started   