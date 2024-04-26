import time
import os
from dataset.data_reader import get_inverter_data
from kafka.producer import create_producer, produce_messages
import logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def produce_inverter_data(producer,
                          device_name,
                          filename="494654",
                          production_interval=1):
    while True:
        msg = get_inverter_data(file_name=filename)
        logger.info(f'Produced {msg}')
        produce_messages(producer=producer, topic=device_name, msg=msg)
        time.sleep(production_interval)
