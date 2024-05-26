import time
import os
from data_reader import get_inverter_data
from kafka.producer import produce_messages
import logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def produce_inverter_data(producer,
                          device_name,
                          df,
                          stop_event,
                          production_interval=1):
    print("Starting producer !")
    while not stop_event.is_set():
        msg = get_inverter_data(df)
        logger.debug(f'Produced: {msg}')
        produce_messages(producer=producer,
                         topic=f"input_{device_name}",
                         msg=msg)
        time.sleep(production_interval)
    logger.info("Kill signal received, flushing producer....")
    producer.flush()

    logger.info("Producer flushed.. Exiting...")
