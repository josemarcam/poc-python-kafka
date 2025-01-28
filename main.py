from typer import Typer, echo
from kafka_service import CoreKafka

app: Typer = Typer()

SERVER_ADDRESS = 'localhost:29092'

@app.command()
def produce(message: str, topic: str):
    echo(f"Produce message {message} at topic {topic}")
    core_kafka = CoreKafka(SERVER_ADDRESS)
    producer = core_kafka.producer()
    producer.send(topic, {"payload": {
        "user_id": 1
    }})
    echo("Message sent")
    

@app.command()
def consume(topic: str, groupid: str = None, mode: str = 'latest'):
    echo(f"Consume message at group {groupid} at topic {topic}")
    core_kafka = CoreKafka(SERVER_ADDRESS)
    consumer = core_kafka.consumer(groupid, mode)
    topic_list = topic.split(',')
    consumer.subscribe(topic_list)
    for msg in consumer:
        data = {
            "topic": msg.topic,
            "value": msg.value
        }
        echo(data)


@app.command()
def create_topic(topic: str, partitions: int = 1, replication_factor: int = 1):
    core_kafka = CoreKafka(SERVER_ADDRESS)
    reponse = core_kafka.create_topic(topic, partitions, replication_factor)
    if reponse:
        echo("Topic created")
    else:
        echo("Topic creation Failed")


if __name__ == '__main__':
    app()