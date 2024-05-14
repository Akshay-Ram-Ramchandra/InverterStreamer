import configparser
import json

from confluent_kafka import Producer
import pandas as pd
import datetime
import time


def delivery_report(err, msg):
    """
    Checks if the error is None, if so assumes no error.
    :param err: Error
    :param msg: Error message
    :return: None prints out delivery status
    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    # else:
    #     print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def create_producer(host, port, username=None, password=None):
    """
    Creates a kafka producer, for a given host and port
    :param host: string, serverip:portnumber
    :return: kafka producer instance
    """
    conf = {
        'bootstrap.servers': host + ":" + port,
        'acks': 'all',
        'retries': 5,
        'retry.backoff.ms': 500,
        'enable.idempotence': True
    }
    # Add username and password if provided
    if username and password:
        conf['security.protocol'] = 'SASL_PLAINTEXT'
        conf['sasl.mechanism'] = 'PLAIN'
        conf['sasl.username'] = username
        conf['sasl.password'] = password

    producer = Producer(conf)

    return producer


def produce_messages(producer, topic, msg):
    producer.poll(0)
    producer.produce(topic, json.dumps(msg), callback=delivery_report)
    producer.flush()
