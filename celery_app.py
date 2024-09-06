from celery import Celery


from _app.download_h8l1 import download_himawari_l1
from _app.download_l2arp import download_himawari_l2arp
from _app.preprocess_h8 import preprocess_h8_data
from _app.download_mod08 import download_mod08_data
from _app.process_mod08 import process_mod08_data
from _app.atmospheric_correction import atmospheric_correction

app = Celery('Celery_Himawari8', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')
# app.autodiscover_tasks(['_app.tasks'], force=True)  # 自动发现 _app 包中的任务

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

@app.task
def download_h8l1_task(date, hour):
    download_himawari_l1(date, hour)
    return 0

@app.task
def download_l2arp_task(date, hour):
    download_himawari_l2arp(date, hour)
    return 0

@app.task
def preprocess_h8_task(date, hour):
    return preprocess_h8_data(date, hour)

@app.task
def download_mod08_task(date):
    download_directory = "downloaded_data/mod08"
    return download_mod08_data(date, download_directory)

@app.task
def process_mod08_task(date, hour):
    return process_mod08_data(date, hour)

@app.task
def atmospheric_correction_task(date, hour):
    return atmospheric_correction(date, hour)