from celery import shared_task
from .download_h8 import download_himawari_data
from .preprocess_h8 import preprocess_h8_data
from .download_mod08 import download_mod08_data
from .process_mod08 import process_mod08_data
from .atmospheric_correction import atmospheric_correction

@shared_task
def download_h8_task(date, hour):
    return download_himawari_data(date, hour)

@shared_task
def preprocess_h8_task(date, hour):
    return preprocess_h8_data(date, hour)

@shared_task
def download_mod08_task(date):
    download_directory = "downloaded_data/mod08"
    return download_mod08_data(date, download_directory)

@shared_task
def process_mod08_task(date, hour):
    return process_mod08_data(date, hour)

@shared_task
def atmospheric_correction_task(date, hour):
    return atmospheric_correction(date, hour)


