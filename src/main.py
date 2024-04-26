import os
from dataset.data_reader import get_inverter_data
import logging
from kafka.producer import create_producer
from data_production import produce_inverter_data
import configparser
import threading


config = configparser.ConfigParser()
config.read('config.ini')
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



produce_to = config['PRODUCETO']

print(produce_to)
producer = create_producer(host=config['KAFKA']['host'],
                           port=config['KAFKA']['port'],
                           username=config['KAFKA']['username'],
                           password=config['KAFKA']['password'])
data_production_threads = []
for device_name in produce_to:
    logger.info(f"Starting production to: {device_name}")
    t = threading.Thread(target=produce_inverter_data,
                         args=(producer,
                               f"inverter_{device_name}",
                               produce_to[device_name],
                               int(config['PRODUCER']['interval'])))
    data_production_threads.append(t)
    t.start()

for t in data_production_threads:
    t.join()
