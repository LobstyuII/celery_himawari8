import logging
import time
from _app.tasks import (
    download_h8_task,
    preprocess_h8_task,
    download_mod08_task,
    process_mod08_task,
    atmospheric_correction_task
)
# 配置主脚本的日志
logging.basicConfig(filename='main_process.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    date_str = '2015-07-13'
    hour = 4

    start_time = time.time()
    logger.info(f"任务开始于 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

    # Step 1: Download H8 Data
    h8_result = download_h8_task.delay(date_str, hour)
    logger.info("启动H8下载任务")

    # Step 2: Preprocess H8 Data
    preprocess_h8_task.delay(date_str, hour)
    logger.info("启动H8数据预处理任务")

    # Step 3: Download MOD08 Data
    mod08_result = download_mod08_task.delay(date_str)
    logger.info("启动MOD08下载任务")

    # Step 4: Process MOD08 Data
    process_mod08_task.delay(date_str, hour)
    logger.info("启动MOD08数据处理任务")

    # Step 5: Atmospheric Correction
    atmospheric_correction_task.delay(date_str, hour)
    logger.info("启动大气校正任务")

    end_time = time.time()
    logger.info(f"任务结束于 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
    logger.info(f"总用时: {end_time - start_time:.2f} 秒")
