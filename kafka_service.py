from typing import List
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json

class CoreKafka:

    def __init__(self, bootstrap_servers):
        self._bootstrap_servers = bootstrap_servers
        self._kafka_admin = KafkaAdminClient(bootstrap_servers=self._bootstrap_servers)

    def create_topic(self, topic, partitions, replication_factor=2) -> bool:
        if topic in self.list_topics():
            return True

        max_replication_factor = len(self.list_brokers())

        if replication_factor > max_replication_factor:
            replication_factor = max_replication_factor

        topic = NewTopic(topic, partitions, replication_factor)

        create_topic_response = self._kafka_admin.create_topics([topic])
        return create_topic_response.topic_errors[0][1] == 0


    def consumer(self, group_id = None, mode='latest') -> KafkaConsumer:
        consumer = KafkaConsumer(
            bootstrap_servers=self._bootstrap_servers, 
            group_id= group_id,
            auto_offset_reset=mode,
            value_deserializer=lambda v: json.loads(v)
        )
        return consumer

    def producer(self) -> KafkaProducer:
        producer = KafkaProducer(
            api_version=(0, 9),
            bootstrap_servers=self._bootstrap_servers, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer

    def list_topics(self) -> List:
        return self._kafka_admin.list_topics()

    def list_brokers(self):
        metadata = self._kafka_admin.describe_cluster()
        return metadata['brokers']