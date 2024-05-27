import signal
import threading
import os
import logging
import time
from dotenv import load_dotenv
from kafka.consumer import create_consumer, consume_messages
from kafka.producer import create_producer, produce_messages, produce_messages_str
from kafka.kafka_admin import create_topic
from thread_manager import manage_threads

load_dotenv()

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
PRODUCE_TO = os.getenv("PRODUCE_TO")
PRODUCTION_INTERVAL = os.getenv("PRODUCTION_INTERVAL")

log_level = os.getenv('LOG_LEVEL', 'DEBUG').upper()
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

stop_flag = threading.Event()


def signal_handler(signum, frame):
    logger.info("Signal received, shutting down...")
    stop_flag.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

produce_to = PRODUCE_TO.split(",")

logger.info(f"The inverter Streamer will generate data for: {produce_to}")

producer = create_producer(host=KAFKA_HOST,
                           port=KAFKA_PORT)

topics = [
    "nano01_stream_file",
    "nano02_stream_file",
    "nano03_stream_file",
    "nano04_stream_file",
    "nano05_stream_file",
    "nano06_stream_file",
]
logger.info(f"Starting topic creation: {topics}")
for topic in topics:
    logger.info(f"Creating topic: {topics}")
    create_topic(kafka_host=KAFKA_HOST, kafka_port=KAFKA_PORT, topic_name=topic)

consumer = create_consumer(host=KAFKA_HOST, port=KAFKA_PORT, topics=topics, group="stream_file_inverter_Streamer")

current_stream_file = {
    "nano01": "Awaiting Start",
    "nano02": "Awaiting Start",
    "nano03": "Awaiting Start",
    "nano04": "Awaiting Start",
    "nano05": "Awaiting Start",
    "nano06": "Awaiting Start",

}

event_map = {
    "nano01": threading.Event(),
    "nano02": threading.Event(),
    "nano03": threading.Event(),
    "nano04": threading.Event(),
    "nano05": threading.Event(),
    "nano06": threading.Event(),

}

thread_map = {
    "nano01": None,
    "nano02": None,
    "nano03": None,
    "nano04": None,
    "nano05": None,
    "nano06": None,

}

while not stop_flag.is_set():
    stream_file = consume_messages(consumer, timeout=1)
    if stream_file:
        if stream_file.error():
            pass
        else:
            consumed_from = stream_file.topic()
            to_update_device = consumed_from.split("_")[0]
            logger.info(f"Received update: {to_update_device}: {stream_file.value().decode('utf-8')}")
            manage_threads(stream_file,
                           to_update_device,
                           current_stream_file,
                           producer,
                           thread_map,
                           event_map)

    # if nano01_stream_file:
    #     manage_threads(nano01_stream_file,
    #                    "nano01",
    #                    current_stream_file,
    #                    producer,
    #                    thread_map,
    #                    event_map)
    # else:
    #     produce_messages_str(producer, "nano01_stream_ack", msg=str(current_stream_file['nano01']))
    #
    # if nano02_stream_file:
    #     manage_threads(nano02_stream_file,
    #                    "nano02",
    #                    current_stream_file,
    #                    producer,
    #                    thread_map,
    #                    event_map)
    # else:
    #     produce_messages_str(producer, "nano02_stream_ack", msg=current_stream_file['nano02'])
    #
    # if nano03_stream_file:
    #     manage_threads(nano03_stream_file,
    #                    "nano03",
    #                    current_stream_file,
    #                    producer,
    #                    thread_map,
    #                    event_map)
    # else:
    #     produce_messages_str(producer, "nano03_stream_ack", msg=current_stream_file['nano03'])
    #
    # if nano04_stream_file:
    #     manage_threads(nano04_stream_file,
    #                    "nano04",
    #                    current_stream_file,
    #                    producer,
    #                    thread_map,
    #                    event_map)
    # else:
    #     produce_messages_str(producer, "nano04_stream_ack", msg=current_stream_file['nano04'])
    #
    # if nano05_stream_file:
    #     manage_threads(nano05_stream_file,
    #                    "nano05",
    #                    current_stream_file,
    #                    producer,
    #                    thread_map,
    #                    event_map)
    # else:
    #     produce_messages_str(producer, "nano05_stream_ack", msg=current_stream_file['nano05'])
    #
    # if nano06_stream_file:
    #     manage_threads(nano06_stream_file,
    #                    "nano06",
    #                    current_stream_file,
    #                    producer,
    #                    thread_map,
    #                    event_map)
    # else:
    #     produce_messages_str(producer, "nano06_stream_ack", msg=current_stream_file['nano06'])
    # time.sleep(1)
    logger.info(f"=============================================")
    stop_flag.wait(1)

logger.info("Cleaning up and exiting.")

logger.info("Killing threads...")
for event in event_map:
    logger.info(f"Killing {event}...")
    event_map[event].set()
logger.info(f"All threads killed...")
if consumer:
    logger.info("Closing consumer...")

    try:
        consumer.unsubscribe()
        logger.info("Consumer unsubscribed from topics.")
    except Exception as e:
        logger.error(f"Error during consumer unsubscribe: {e}")
    consumer.close()
    logger.info("Consumer Closed..")

if producer:
    logger.info("Flushing Producer...")
    try:
        producer.flush()
        logger.info("Producer Flushed.")
    except Exception as e:
        logger.error(f"Error flushing producer: {e}")
logger.info("Exiting...")

# data_production_threads = []
# for device_name in produce_to:
#     logger.info(f"Starting production to: {device_name}")
# t = threading.Thread(target=produce_inverter_data,
#                      args=(producer,
#                            f"input_{device_name}",
#                            os.getenv(device_name),
#                            int(PRODUCTION_INTERVAL)),
#                      name="nano01")
#     data_production_threads.append(t)
#     t.start()
#
# for t in data_production_threads:
#     t.join()
