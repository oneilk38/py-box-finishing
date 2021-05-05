from confluent_kafka import Consumer, KafkaError, KafkaException, admin, Message
from functools import partial
import pickticket_release
import pickticket_summary
import pick_complete


# Kafka Config
from Exceptions.exns import InvalidPickTicketStateException, PickTicketNotFoundException, PoisonMessageException

import sys

from contracts import Packed, packed_schema

sys.path.append('/app')

from app import db

consumer_conf = {'bootstrap.servers': "broker:29092",
        'group.id': "kon-local-consumer",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(consumer_conf)

# Create Topics
kafka_admin = admin.AdminClient({'bootstrap.servers': 'broker:29092'})

new_topic = admin.NewTopic('python-local-topic', 1, 1)
pick_ticket_summary_topic = admin.NewTopic('pt-summary', 1, 1)


pick_ticket_released_topic = admin.NewTopic('pt-release', 1, 1)
pick_completed_topic = admin.NewTopic('pick-completed', 1, 1)

kafka_admin.create_topics([new_topic, pick_ticket_summary_topic, pick_ticket_released_topic, pick_completed_topic])

# Wire Up Handlers
handle_pickticket_summary = partial(pickticket_summary.handle_pickticket_summary,
                                    pickticket_summary.persist_summary)

handle_pickticket_released = partial(pickticket_release.handle_pickticket_released,
                                     pickticket_release.persist_released)

handle_pick_completed = partial(pick_complete.handle_pick_completed,
                                pick_complete.persist_pick_complete)

# Handle Message
def handle_other(msg: Message):
    print(f'Consuming {new_topic.topic}....')
    packed = packed_schema.loads(msg.value())
    print(packed)


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


def handle(msg: Message):
    if msg.topic() == pick_ticket_summary_topic.topic:
        handle_with_retry(handle_pickticket_summary, msg)
    elif msg.topic() == pick_ticket_released_topic.topic:
        handle_with_retry(handle_pickticket_released, msg, 0)
    elif msg.topic() == pick_completed_topic.topic:
        handle_with_retry(handle_pick_completed, msg)
    else:
        handle_other(msg)


# Run Consumer
def run_consumer():
    try:
        consumer.subscribe([
            new_topic.topic,
            pick_ticket_summary_topic.topic,
            pick_ticket_released_topic.topic,
            pick_completed_topic.topic
        ])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error(): raise KafkaException(msg.error())
            else:           handle(msg)
    finally:
        consumer.close()


run_consumer()
