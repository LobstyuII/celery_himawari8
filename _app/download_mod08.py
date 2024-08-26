import os
import requests
import logging
from tqdm import tqdm
from datetime import datetime

# 配置日志记录
logging.basicConfig(filename='mod08_download.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger()



API_TOKEN_FILE = 'my_NASA_API_token'
MOD08_DOWNLOAD_LIST_FILE = 'MOD08_download_list.txt'

# API_TOKEN_FILE = 'my_NASA_API_token'
# MOD08_DOWNLOAD_LIST_FILE = 'MOD08_download_list.txt'


def load_nasa_api_token(token_file=API_TOKEN_FILE):
    with open(token_file, 'r') as file:
        token = file.read().strip()

        return token


API_TOKEN = load_nasa_api_token()

def find_closest_file(urls, target_date):
    target_str = target_date.strftime("%Y%j")
    for url in urls:
        file_name = url.split('/')[-1]
        date_str = file_name.split('.')[1][1:]
        if date_str == target_str:
            return url, file_name
    return None, None


def download_file(url, filename, download_dir):
    filename = filename.strip()
    local_filepath = os.path.join(download_dir, filename)
    headers = {'Authorization': f'Bearer {API_TOKEN}'}
    print("加载API中")

    response = requests.get(url.strip(), headers=headers, stream=True)
    response.raise_for_status()
    print("加载成功")

    total_size = int(response.headers.get('content-length', 0))
    with open(local_filepath, 'wb') as file, tqdm(
            desc=filename,
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
    ) as pbar:
        for data in response.iter_content(chunk_size=1024):
            file.write(data)
            pbar.update(len(data))

    return local_filepath


def download_mod08_data(date, download_dir):
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    with open(MOD08_DOWNLOAD_LIST_FILE, 'r') as f:
        urls = f.readlines()

    url, file_name = find_closest_file(urls, date)
    print("配置url:",url)

    if url and file_name:
        downloaded_file_path = download_file(url.strip(), file_name, download_dir)
        if downloaded_file_path:
            new_filename = f"MOD08_{date.strftime('%Y%m%d')}.hdf"
            new_filepath = os.path.join(download_dir, new_filename)
            os.rename(downloaded_file_path, new_filepath)
            return new_filepath
    return None
