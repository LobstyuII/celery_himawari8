from celery import Celery

# app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
app = Celery('tasks', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(task_name)s[%(task_id)s]: %(message)s',
    worker_redirect_stdouts_level='INFO',  # 确保 INFO 级别的输出被显示
)
