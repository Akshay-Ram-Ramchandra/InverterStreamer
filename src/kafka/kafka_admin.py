from confluent_kafka.admin import AdminClient, NewTopic
import logging
logger = logging.getLogger(__name__)

def list_kafka_topics(bootstrap_servers):
    """
    Lists all topics in the Kafka cluster.

    Args:
    bootstrap_servers (str): A string of bootstrap servers in the format 'host1:port,host2:port'.
    """
    # Create an AdminClient using the bootstrap servers
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # List all topics.
    topic_metadata = admin_client.list_topics(timeout=10)  # Timeout in seconds

    # Print the names of all topics
    print("Kafka Topics:")
    for topic in topic_metadata.topics:
        print(topic)


def create_topic(kafka_host,
                 kafka_port,
                 topic_name,
                 partitions=1,
                 replication_factor=1):
    conf = {
        'bootstrap.servers': kafka_host + ":" + kafka_port,
    }

    admin_client = AdminClient(conf)

    topic_name = topic_name
    num_partitions = partitions
    replication_factor = replication_factor

    new_topic = NewTopic(topic=topic_name,
                         num_partitions=num_partitions,
                         replication_factor=replication_factor)

    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"Topic {topic} created successfully")
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    # Example usage: replace 'localhost:9092' with your Kafka broker's address
    list_kafka_topics('localhost:9092')
