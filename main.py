import logging
import time
import datetime
from celery_app import (
    download_h8l1_task,
    download_l2arp_task,
    preprocess_h8_task,
    download_mod08_task,
    process_mod08_task,
    atmospheric_correction_task
)
import eventlet
eventlet.monkey_patch()

# 配置主脚本的日志
logging.basicConfig(filename='main_process.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

## 请在terminal中使用以下代码
## celery -A celery_app worker --loglevel=info -P eventlet

if __name__ == "__main__":
    date = datetime.date(2015, 7, 16)
    hour = 4


    start_time = time.time()
    logger.info(f"任务开始于 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

    # Step 1: Download H8L1 Data
    h8_result = download_h8l1_task.delay(date, hour)
    logger.info("启动H8L1下载任务")

    # Step 2: Download H8L2arp Data
    h8_result = download_l2arp_task.delay(date, hour)
    logger.info("启动H8L2ARP下载任务")

    # Step 2: Preprocess H8 Data
    # preprocess_h8_task.delay(date, hour)
    logger.info("启动H8数据预处理任务")

    # Step 3: Download MOD08 Data
    # mod08_result = download_mod08_task.delay(date)
    logger.info("启动MOD08下载任务")

    # Step 4: Process MOD08 Data
    # process_mod08_task.delay(date, hour)
    logger.info("启动MOD08数据处理任务")

    # Step 5: Atmospheric Correction
    # atmospheric_correction_task.delay(date, hour)
    logger.info("启动大气校正任务")

    end_time = time.time()
    logger.info(f"任务结束于 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
    logger.info(f"总用时: {end_time - start_time:.2f} 秒")
