import os
import ftplib
import logging
from tqdm import tqdm
import datetime
import time

# 设置日志配置
logging.basicConfig(filename='h8_download.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# FTP连接信息
FTP_ADDRESS = "ftp.ptree.jaxa.jp"
FTP_UID = "511153727_qq.com"
FTP_PW = "SP+wari8"

def download_from_ftp(ftp_path, local_filename, download_dir, max_retries=5, retry_delay=10):
    logger.info(f"开始从FTP服务器下载文件: {ftp_path}")

    # 创建下载目录
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    local_filepath = os.path.join(download_dir, local_filename)
    resume_byte_pos = 0

    # 如果文件已经存在，检查文件大小用于断点续传
    if os.path.exists(local_filepath):
        resume_byte_pos = os.path.getsize(local_filepath)
        logger.info(f"文件已存在，准备从 {resume_byte_pos} 字节处继续下载。")

    attempt = 0
    while attempt < max_retries:
        try:
            with ftplib.FTP(FTP_ADDRESS, timeout=retry_delay) as ftp:
                ftp.login(FTP_UID, FTP_PW)
                ftp.voidcmd("TYPE I")
                file_size = ftp.size(ftp_path)

                if resume_byte_pos >= file_size:
                    logger.info("文件已完整下载，无需重新下载。")
                    return local_filepath

                with open(local_filepath, 'ab') as local_file:
                    with tqdm(total=file_size, unit='B', unit_scale=True, initial=resume_byte_pos,
                              desc=local_filename) as pbar:
                        def callback(data):
                            local_file.write(data)
                            pbar.update(len(data))

                        # 断点续传功能，RETR命令从指定字节位置开始下载
                        ftp.retrbinary(f'RETR {ftp_path}', callback, rest=resume_byte_pos)

            logger.info(f"文件下载成功: {local_filepath}")
            return local_filepath

        except ftplib.all_errors as e:
            logger.error(f"FTP文件下载失败: {e}, 重试 {attempt + 1}/{max_retries}...")
            attempt += 1
            time.sleep(retry_delay)

    logger.error(f"文件下载失败: 超过最大重试次数 {max_retries}")
    return None

def download_himawari_data(date, hour):
    logger.info(f"开始下载Himawari-8 L1数据 for {date} {hour}:00 UTC")
    ftp_path = f"/jma/netcdf/{date.year:04d}{date.month:02d}/{date.day:02d}/NC_H08_{date.year:04d}{date.month:02d}{date.day:02d}_{hour:02d}00_R21_FLDK.02401_02401.nc"

    logger.info(f"ftp_path: {ftp_path}")

    local_filename = f"himawari_{date.strftime('%Y%m%d')}_{hour:02d}.nc"
    download_dir = "downloaded_data/h8l1"
    return download_from_ftp(ftp_path, local_filename, download_dir)

if __name__ == '__main__':
    date = datetime.date(2015, 7, 14)
    hour = 4
    download_himawari_data(date, hour)
