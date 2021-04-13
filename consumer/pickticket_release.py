import typing, sys

sys.path.append('/app')
from dataclasses import dataclass, field
from functools import partial

from confluent_kafka import Message
from app import db
from Models.allocations_by_pickticket import AllocationsByPickTicket
from Models.requested_items_by_pickticket import RequestedItemsByPickTicket

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


def to_allocation_by_pickticket(pickticket_id, fcid, item: PickItem) -> AllocationsByPickTicket:
    return AllocationsByPickTicket(pickticket_id=pickticket_id,
                                   fcid=fcid,
                                   gtin=item.gtin,
                                   quantity=item.quantity)


def to_requested_item_by_pickticket(pickticket_id, fcid, item: PickItem) -> RequestedItemsByPickTicket:
    return RequestedItemsByPickTicket(pickticket_id=pickticket_id,
                                      fcid=fcid,
                                      gtin=item.gtin,
                                      quantity=item.quantity)


def persist_allocations(pt_released: PickTicketReleased):
    to_allocation = partial(to_allocation_by_pickticket, pt_released.pickTicketId, pt_released.fcId)
    allocations = map(to_allocation, pt_released.allocations['asrs'])
    try:
        db.session.add_all(allocations)
        db.session.commit()
        print(f'Successfully added allocations to the database for PickTicket {pt_released.pickTicketId}')
    except Exception as err:
        db.session.rollback()
        print(f'Failed to add allocations to DB for PickTicket {pt_released.pickTicketId}, {err}')


def persist_requested_items(pt_released: PickTicketReleased):
    to_requested_item = partial(to_requested_item_by_pickticket, pt_released.pickTicketId, pt_released.fcId)
    requested_items = map(to_requested_item, pt_released.requestedItems)

    try:
        db.session.add_all(requested_items)
        db.session.commit()
        print(f'Successfully added requested items to the database for PickTicket {pt_released.pickTicketId}')
    except Exception as err:
        db.session.rollback()
        print(f'Failed to add requested items to DB for PickTicket {pt_released.pickTicketId}, {err}')


def handle_pickticket_released(topic, msg: Message):
    print(f'Consuming {topic}....')
    try:
        pt_released: PickTicketReleased = pt_release_schema.loads(msg.value())
        print(f'Successfully deserialised pt released message, {pt_released}')

        persist_allocations(pt_released)
        persist_requested_items(pt_released)
    except Exception as err:
        print(f'failed to deserialise, {err}')
