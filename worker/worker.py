from dataclasses import field

import marshmallow_dataclass
from confluent_kafka import Consumer, KafkaError, KafkaException, admin, Message
from functools import partial

# Kafka Config
from marshmallow import EXCLUDE

import pack

import sys
sys.path.append('/app')
from contracts import Action, action_schema
from Exceptions.exns import InvalidPickTicketStateException, PickTicketNotFoundException, PoisonMessageException


from app import db
consumer_conf = {'bootstrap.servers': "broker:29092",
        'group.id': "kon-local-worker",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(consumer_conf)

# Create Topics
kafka_admin = admin.AdminClient({'bootstrap.servers': 'broker:29092'})

worker_topic = admin.NewTopic('pickticket-events', 1, 1)

kafka_admin.create_topics([worker_topic])

def handle_with_retry(handle, msg, retry_attempt=0):
    def retry(attempt, error_msg):
        if attempt < 5:
            handle_with_retry(handle, msg, attempt)
        else:
            print(error_msg)
            # produce to a DLQ (topic, msg, err)
    try:
        handle(msg)
    except PoisonMessageException as poison:
        print(f'Ignoring message as cant process, {poison}')
    except PickTicketNotFoundException as not_found:
        err_msg = f'Pick Ticket does not exist, ignoring message, {not_found}'
        retry(retry_attempt+1, err_msg)
    except InvalidPickTicketStateException as invalid_state:
        err_msg = f'Ignoring message as invalid state, {invalid_state}'
        retry(retry_attempt + 1, err_msg)
    except Exception as err:
        db.session.rollback()
        err_msg = f'Failed to add to DB, {err}, {msg.value()}'
        retry(retry_attempt + 1, err_msg)


def deserialise(msg: Message):
    try:
        return action_schema.loads(msg.value())
    except Exception as err:
        raise PoisonMessageException(f'Failed to deserialise, Cannot process this message, {msg.value()}, err: {err}')


def handle(msg: Message):
    if msg.topic() == worker_topic.topic:
        action: Action = deserialise(msg)
        if action.type == "PACKED": handle_with_retry(pack.handle, action)
        else:
            print("Unknown action, ignoring...")
    else:
        print("Unknown topic, ignoring...")


def run_consumer():
    try:
        consumer.subscribe([worker_topic.topic])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                handle(msg)
    finally:
        consumer.close()


run_consumer()
