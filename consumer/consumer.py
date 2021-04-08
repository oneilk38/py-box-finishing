from confluent_kafka import Consumer, KafkaError, KafkaException, admin, Message
from functools import partial
import pickticket_release
import pickticket_summary
import pick_complete

# Kafka Config
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
handle_pickticket_summary = partial(pickticket_summary.handle_pickticket_summary, pick_ticket_summary_topic.topic)
handle_pickticket_released = partial(pickticket_release.handle_pickticket_released, pick_ticket_released_topic.topic)
handle_pick_completed = partial(pick_complete.handle_pick_completed, pick_completed_topic.topic)


# Handle Message
def handle_other(msg: Message):
    print(f'Consuming {new_topic.topic}....')
    print(f'{msg.value()}')


def handle(msg: Message):
    if msg.topic() == pick_ticket_summary_topic.topic:
        handle_pickticket_summary(msg)
    elif msg.topic() == pick_ticket_released_topic.topic:
        handle_pickticket_released(msg)
    elif msg.topic() == pick_completed_topic.topic:
        handle_pick_completed(msg)
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
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                handle(msg)
    finally:
        consumer.close()


run_consumer()
