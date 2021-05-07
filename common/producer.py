from confluent_kafka import Producer

def produce(broker, topic, json, key):
    producer_conf = {
        'bootstrap.servers': broker,
        'client.id': 'kon-local-python-producer'
    }

    producer = Producer(producer_conf)
    producer.produce(topic, key=key, value=json)
    producer.poll(1)
