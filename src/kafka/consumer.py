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


def create_consumer(host,
                    port,
topics,
                    group='my_consumer',
                    username=None,
                    password=None):
    conf = {
        'bootstrap.servers': f'{host}:{port}',
        'group.id': group,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'group.instance.id': f'unique-{group}-instance'
    }
    if username and password:
        conf.update({
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password
        })
    try:
        consumer = Consumer(conf)
        print("Consumer created successfully!")
        consumer.subscribe(topics)
        print("Consumer created successfully!")
        return consumer
    except Exception as e:
        print(f"Failed to create consumer: {str(e)}")
        return None


def consume_messages(consumer, timeout=1.5):

    try:
        # msgs = consumer.consume(timeout=1.5, num_messages=nmsgs)
        msgs = consumer.poll(timeout=timeout)
        return msgs
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
