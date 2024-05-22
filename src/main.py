import signal
import threading
import os
import logging
import time

from dotenv import load_dotenv
from aws_s3.read_s3 import get_csv_for_stream
from kafka.consumer import create_consumer, consume_messages
from kafka.producer import create_producer
from data_production import produce_inverter_data


load_dotenv()

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
PRODUCE_TO = os.getenv("PRODUCE_TO")
PRODUCTION_INTERVAL = os.getenv("PRODUCTION_INTERVAL")

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
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

consumer = create_consumer(host=KAFKA_HOST,
                           port=KAFKA_PORT,
                           group="inverter_streamer")

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
    nano01_stream_file = consume_messages(consumer, "nano01_stream_file")
    nano02_stream_file = consume_messages(consumer, "nano02_stream_file")
    nano03_stream_file = consume_messages(consumer, "nano03_stream_file")
    nano04_stream_file = consume_messages(consumer, "nano04_stream_file")
    nano05_stream_file = consume_messages(consumer, "nano05_stream_file")
    nano06_stream_file = consume_messages(consumer, "nano06_stream_file")

    if nano01_stream_file:
        if nano01_stream_file.error():
            err = nano01_stream_file.value().decode("utf-8")
            if err.split(":")[0] == "Subscribed topic not available":
                logger.info(f"Nano01, Awaiting topic Creation")
            else:
                logger.error(f"Nano01: {err}")

        else:
            file_to_stream = nano01_stream_file.value().decode("utf-8")
            currently_streaming = current_stream_file["nano01"]
            logger.info(f"Nano01 is streaming: {currently_streaming}")
            producer.produce(topic="nano01_stream_ack", value=f"Currently streaming: {currently_streaming}")

            if file_to_stream != currently_streaming:
                logger.info(f"Nano01 will be switched to: {file_to_stream}")
                producer.produce(topic="nano01_stream_ack", value=f"Will be switched to: {file_to_stream}")

                logger.info(f"Nano01: Reading file: {file_to_stream}.")
                producer.produce(topic="nano01_stream_ack", value=f"File Download started.")
                df = get_csv_for_stream(file_to_stream)
                producer.produce(topic="nano01_stream_ack", value=f"File Download Completed.")
                logger.info(f"Nano01: {file_to_stream} Read.")
                # Stop the old thread and start a new one, that as the new df as a parameter.
                if not thread_map["nano01"]:
                    logger.info("Starting Fresh stream..")
                    t = threading.Thread(target=produce_inverter_data,
                                         args=(producer,
                                               "nano01",
                                               df,
                                               event_map["nano01"],
                                               1),
                                         name="nano01")
                    thread_map["nano01"] = t
                    thread_map["nano01"].start()

                else:
                    logger.info(f"Killing old producer...")
                    event_map["nano01"].set()
                    while thread_map["nano01"].is_alive():
                        time.sleep(1)
                        logger.info("Awaiting process completion...")
                    # unsetting the event
                    event_map["nano01"].clear()
                    t = threading.Thread(target=produce_inverter_data,
                                         args=(producer,
                                               "nano01",
                                               df,
                                               event_map["nano01"],
                                               1),
                                         name="nano01")
                    t.start()
                    thread_map["nano01"] = t


                    logger.info("New thread started")
                current_stream_file["nano01"] = file_to_stream
                producer.produce(topic="nano01_stream_ack", value=f"Commenced start: {file_to_stream}")


            else:
                logger.info(f"Nano01 will not be switched{file_to_stream} = {current_stream_file['nano01']}")




    # if nano02_stream_file:
    #     if nano02_stream_file.error():
    #         err = nano02_stream_file.value().decode("utf-8")
    #         if err.split(":")[0] == "Subscribed topic not available":
    #             logger.info(f"Nano02, Awaiting topic Creation")
    #         else:
    #             logger.error(f"Nano02: {err}")
    #
    #     else:
    #         file_to_stream = nano02_stream_file.value().decode("utf-8")
    #
    #         logger.info(f"Nano02: I will stream {file_to_stream}.")
    #
    # if nano03_stream_file:
    #     if nano03_stream_file.error():
    #         err = nano03_stream_file.value().decode("utf-8")
    #         if err.split(":")[0] == "Subscribed topic not available":
    #             logger.info(f"Nano03, Awaiting topic Creation")
    #         else:
    #             logger.error(f"Nano03: {err}")
    #
    #     else:
    #         file_to_stream = nano03_stream_file.value().decode("utf-8")
    #
    #         logger.info(f"Nano03: I will stream {file_to_stream}.")
    #
    # if nano04_stream_file:
    #     if nano04_stream_file.error():
    #         err = nano04_stream_file.value().decode("utf-8")
    #         if err.split(":")[0] == "Subscribed topic not available":
    #             logger.info(f"Nano04, Awaiting topic Creation")
    #         else:
    #             logger.error(f"Nano04: {err}")
    #
    #     else:
    #         file_to_stream = nano04_stream_file.value().decode("utf-8")
    #
    #         logger.info(f"Nano04: I will stream {file_to_stream}.")
    #
    # if nano05_stream_file:
    #     if nano05_stream_file.error():
    #         err = nano05_stream_file.value().decode("utf-8")
    #         if err.split(":")[0] == "Subscribed topic not available":
    #             logger.info(f"Nano05, Awaiting topic Creation")
    #         else:
    #             logger.error(f"Nano05: {err}")
    #
    #     else:
    #         file_to_stream = nano04_stream_file.value().decode("utf-8")
    #
    #         logger.info(f"Nano05: I will stream {file_to_stream}.")
    #
    # if nano06_stream_file:
    #     if nano06_stream_file.error():
    #         err = nano06_stream_file.value().decode("utf-8")
    #         if err.split(":")[0] == "Subscribed topic not available":
    #             logger.info(f"Nano06, Awaiting topic Creation")
    #         else:
    #             logger.error(f"Nano06: {err}")
    #
    #     else:
    #         file_to_stream = nano04_stream_file.value().decode("utf-8")
    #
    #         logger.info(f"Nano06: I will stream {file_to_stream}.")

    logger.info(f"=============================================")
    stop_flag.wait(1) # Wait for 1 second before checking again to prevent tight loop

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
