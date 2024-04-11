import os
import json
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
import uuid

hostip = os.getenv('HOSTNAME')
if not hostip:
    hostip = os.popen('hostname -I').read()

hostip = hostip.strip().split(" ")[0]

username = "myuser"
password = "mysecretpassword"


def create_consumer(host, port, group='my_consumer'):
    conf = {
        'bootstrap.servers': host + ':' + str(port),
        'group.id': group,  # Consumer group ID
        'auto.offset.reset': 'latest',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': username,
        'sasl.password': password
    }
    consumer = Consumer(**conf)
    return consumer


def consume_messages(consumer, topic, nmsgs=10):
    consumer.subscribe([topic])
    try:
        msgs = consumer.consume(num_messages=nmsgs)
        return [[msg.value().decode('utf-8') for msg in msgs], [msg.offset() for msg in msgs]]
    except Exception as e:
        print("Error" + str(e))
        return "Error" + str(e)


def delete_consumer_group(host, group_id, port=9092):
    admin_client = AdminClient({
        'bootstrap.servers': host + ":" + str(port)
    })

    fut = admin_client.delete_consumer_groups([group_id])

    try:
        result = fut[group_id].result()  # The result itself is an empty object on success
        print(f"Consumer group '{group_id}' deleted successfully.")
    except Exception as e:
        print(f"Failed to delete consumer group '{group_id}': {e}")


if __name__ == '__main__':
    group = "my_consumer"
    consumer = create_consumer(hostip, 9092, group=group)
    # Topic to consume messages from
    topic = 'input_topic'
    continue_from = False
    while True:
        try:
            msgs = consume_messages(consumer, topic)
            print(len(msgs[0]))
            print(msgs[1])

        except KeyboardInterrupt:
            consumer.close()
            if not continue_from:
                delete_consumer_group(hostip, group)
            break
