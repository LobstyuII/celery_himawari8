from celery import Celery
import eventlet
eventlet.monkey_patch()

app = Celery('Celery_Himawari8', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')
# app = Celery('tasks', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')
app.autodiscover_tasks(['_app.tasks'], force=True)  # 自动发现 _app 包中的任务
# app.autodiscover_tasks(['_app.tasks'])  # 自动发现 _app 包中的任务



app.conf.update(
    worker_concurrency=32,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    broker_connection_retry_on_startup=True, # 20240830新加入，为了更高版本兼容
    enable_utc=True,
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s',
    worker_redirect_stdouts_level='INFO',
)
