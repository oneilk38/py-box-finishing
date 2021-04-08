import typing
from dataclasses import dataclass, field
from confluent_kafka import Message

import marshmallow_dataclass
from marshmallow import EXCLUDE

# Pick Ticket Released
@dataclass
class PickItem:
    class Meta:
        unknown = EXCLUDE
    gtin: str = field(metadata={}, default='gtin')
    quantity: int = field(metadata={}, default=1)


PickItemList = typing.List[PickItem]
AllocationMap = typing.Dict[str, PickItemList]


@dataclass
class PickTicketReleased:
    class Meta:
        unknown = EXCLUDE
    pickTicketId: str = field(metadata={}, default='ptId')
    fcId: str = field(metadata={}, default='fcId')
    cutOff: str = field(metadata={}, default='cutofftime')
    priority: int = field(metadata={}, default=1)
    requestedItems: typing.List[PickItem] = field(default_factory=list)
    allocations: AllocationMap = field(default_factory=dict)
    timestamp: str = field(metadata={}, default='timestamp')


pt_release_schema = marshmallow_dataclass.class_schema(PickTicketReleased)()


def handle_pickticket_released(topic, msg: Message):
    print(f'Consuming {topic}....')
    print(f'{msg.value()}')
    try:
        pt_released = pt_release_schema.loads(msg.value())
        print(f'Successfully deserialised pt released message')
        print(pt_released)
    except Exception as err:
        print(f'failed to deserialise, {err}')
