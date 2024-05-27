import logging
import os
import threading
import time
from aws_s3.read_s3 import get_csv_for_stream
from data_production import produce_inverter_data


log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def manage_threads(stream_file,
                   device_name,
                   current_stream_file,
                   producer,
                   thread_map,
                   event_map):
    if stream_file:
        if stream_file.error():
            err = stream_file.value().decode("utf-8")
            if err.split(":")[0] == "Subscribed topic not available":
                logger.info(f"{device_name}, Awaiting topic Creation")
            else:
                logger.error(f"{device_name}: {err}")

        else:
            file_to_stream = stream_file.value().decode("utf-8")
            currently_streaming = current_stream_file[device_name]
            logger.info(f"{device_name} is streaming: {currently_streaming}")
            producer.produce(topic=f"{device_name}_stream_ack", value=f"Currently streaming: {currently_streaming}")

            if file_to_stream != currently_streaming:
                logger.info(f"{device_name} will be switched to: {file_to_stream}")
                producer.produce(topic=f"{device_name}_stream_ack", value=f"Will be switched to: {file_to_stream}")

                logger.info(f"{device_name}: Reading file: {file_to_stream}.")
                producer.produce(topic=f"{device_name}_stream_ack", value=f"File Download started.")
                try:
                    df = get_csv_for_stream(file_to_stream)

                    producer.produce(topic=f"{device_name}_stream_ack", value=f"File Download Completed.")
                    logger.info(f"{device_name}: {file_to_stream} Read.")
                    # Stop the old thread and start a new one, that as the new df as a parameter.
                    if not thread_map[device_name]:
                        logger.info("Starting Fresh stream..")
                        producer.produce(topic=f"{device_name}_stream_ack", value="Starting Fresh Stream...")
                        t = threading.Thread(target=produce_inverter_data,
                                             args=(producer,
                                                   device_name,
                                                   df,
                                                   event_map[device_name],
                                                   1),
                                             name=device_name)
                        thread_map[device_name] = t
                        thread_map[device_name].start()
                        logger.info(f"{device_name} thread: {file_to_stream} started.")
                        producer.produce(topic=f"{device_name}_stream_ack", value=f"{stream_file}")

                    else:
                        logger.info(f"Killing old producer...")
                        producer.produce(topic=f"{device_name}_stream_ack", value="Killing old producer...")
                        event_map[device_name].set()
                        while thread_map[device_name].is_alive():
                            time.sleep(1)
                            logger.info("Awaiting process completion...")
                            producer.produce(topic=f"{device_name}_stream_ack", value="Awaiting process completion...")
                        # unsetting the event
                        event_map[device_name].clear()
                        t = threading.Thread(target=produce_inverter_data,
                                             args=(producer,
                                                   device_name,
                                                   df,
                                                   event_map[device_name],
                                                   1),
                                             name=device_name)
                        t.start()
                        thread_map[device_name] = t

                        logger.info("New thread started")
                    current_stream_file[device_name] = file_to_stream
                    producer.produce(topic=f"{device_name}_stream_ack", value=f"Commencing stream: {file_to_stream}")
                    producer.produce(topic=f"{device_name}_stream_ack", value=f"{file_to_stream}")

                except Exception as e:
                    logger.error(f"Stream start failed: {e}. Please re-select.")
                    producer.produce(topic=f"{device_name}_stream_ack", value=f"File download Failed because of: {e}. Please re-select.")
                    producer.flush()
            else:
                logger.info(f"{device_name} will not be switched{file_to_stream} = {current_stream_file[device_name]}")
