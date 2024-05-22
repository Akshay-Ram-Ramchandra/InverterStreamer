import time
import os
from src.data_reader import get_inverter_data
from kafka.producer import produce_messages
import logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def produce_inverter_data(producer,
                          device_name,
                          df,
                          production_interval=1):
    while True:
        msg = get_inverter_data(df)
        logger.info(f'Produced {msg}')
        produce_messages(producer=producer, topic=device_name, msg=msg)
        time.sleep(production_interval)
