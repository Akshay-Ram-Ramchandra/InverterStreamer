import os
from dotenv import load_dotenv
from dataset.data_reader import get_inverter_data
import logging
from kafka.producer import create_producer
from data_production import produce_inverter_data
import threading

load_dotenv()

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
PRODUCE_TO = os.getenv("PRODUCE_TO")
PRODUCTION_INTERVAL = os.getenv("PRODUCTION_INTERVAL")

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

produce_to = PRODUCE_TO.split(",")

logger.info(f"The inverter Streamer will generate data for: {produce_to}")


producer = create_producer(host=KAFKA_HOST,
                           port=KAFKA_PORT)
data_production_threads = []
for device_name in produce_to:
    logger.info(f"Starting production to: {device_name}")
    t = threading.Thread(target=produce_inverter_data,
                         args=(producer,
                               f"input_{device_name}",
                               os.getenv(device_name),
                               int(PRODUCTION_INTERVAL)))
    data_production_threads.append(t)
    t.start()

for t in data_production_threads:
    t.join()
