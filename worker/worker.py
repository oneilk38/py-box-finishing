import inspect
import json, sys
from typing import Dict, List

sys.path.append('/app/consumer')

from confluent_kafka import Consumer, KafkaError, KafkaException, admin
from consumer.pickticket_summary import pt_summary_schema


consumer_conf = {'bootstrap.servers': "broker:29092",
        'group.id': "kon-local-consumer-2",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(consumer_conf)

kafka_admin = admin.AdminClient({'bootstrap.servers': 'broker:29092'})

new_topic = admin.NewTopic('python-local-topic', 1, 1)
pick_ticket_summary_topic = admin.NewTopic('pt-summary', 1, 1)

kafka_admin.create_topics([new_topic, pick_ticket_summary_topic])


def handle_pickticket_summary(msg):
    print(f'Consuming {pick_ticket_summary_topic.topic}....')
    print(f'{msg.value()}')
    try:
        pt_summary = pt_summary_schema.loads(msg.value())
        print(f'Successfully deserialised pt summary message')
        print(pt_summary)



    except Exception as err:
        print(f'failed to deserialise, {err}')


def handle_other(msg):
    print(f'Consuming {new_topic.topic}....')
    print(f'{msg.value()}')




def handle(msg):
    if msg.topic() == pick_ticket_summary_topic.topic:
        handle_pickticket_summary(msg)
    else:
        handle_other(msg)


def run_consumer():
    try:
        consumer.subscribe(["python-local-topic", "pt-summary"])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            else:
                handle(msg)
    finally:
        consumer.close()


run_consumer()
