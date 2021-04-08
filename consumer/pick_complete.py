import typing
from dataclasses import dataclass, field
from confluent_kafka import Message

import marshmallow_dataclass
from marshmallow import EXCLUDE

@dataclass
class Pick:
    class Meta:
        unknown = EXCLUDE
    gtin: str = field(metadata={}, default='gtin')
    quantity: int = field(metadata={}, default=1)

@dataclass
class PickComplete:
    pickTicketId: str = field(metadata={}, default='ptId')
    fcId: str = field(metadata={}, default='fcId')
    sheetId: int = field(default=1)
    containerId: str = field(metadata={}, default='containerId')
    orderId: str = field(metadata={}, default='orderId')
    containerType: str = field(metadata={}, default='containerType')
    destination: str = field(metadata={}, default='destination')
    pickingSource: str = field(metadata={}, default='source')
    picks: typing.List[Pick] = field(default_factory=list)


pick_completed_schema = marshmallow_dataclass.class_schema(PickComplete)()

def handle_pick_completed(topic, msg: Message):
    print(f'Consuming {topic}....')
    print(f'{msg.value()}')
    try:
        pick_completed = pick_completed_schema.loads(msg.value())
        print(f'Successfully deserialised pick completed message')
        print(pick_completed)
    except Exception as err:
        print(f'failed to deserialise, {err}')

