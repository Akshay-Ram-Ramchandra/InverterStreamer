from confluent_kafka.admin import AdminClient

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

if __name__ == "__main__":
    # Example usage: replace 'localhost:9092' with your Kafka broker's address
    list_kafka_topics('localhost:9092')
