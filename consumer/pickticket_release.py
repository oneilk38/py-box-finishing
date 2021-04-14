import typing, sys

sys.path.append('/app')
from dataclasses import dataclass, field

from confluent_kafka import Message
from app import db
from Models.tables import AllocationsByPickTicket, RequestedItemsByPickTicket, PickTicketById

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


def to_allocation_by_pickticket(item: PickItem, pickticket: PickTicketById) -> AllocationsByPickTicket:
    return AllocationsByPickTicket(pickticket_id=pickticket.pickticket_id,
                                   fcid=pickticket.fcid,
                                   gtin=item.gtin,
                                   quantity=item.quantity,
                                   pickticket=pickticket)


def to_allocations_by_pickticket(pickticket: PickTicketById, pt_release: PickTicketReleased):
    allocations = [
        to_allocation_by_pickticket(allocation, pickticket)
        for allocation
        in pt_release.allocations['asrs']
    ]

    return allocations


def to_requested_item_by_pickticket(item: PickItem, pickticket: PickTicketById) -> RequestedItemsByPickTicket:
    return RequestedItemsByPickTicket(pickticket_id=pickticket.pickticket_id,
                                      fcid=pickticket.fcid,
                                      gtin=item.gtin,
                                      quantity=item.quantity,
                                      pickticket=pickticket)


def to_requested_items_by_pickticket(pickticket: PickTicketById, pt_release: PickTicketReleased):
    requested_items = [
        to_requested_item_by_pickticket(requested_item, pickticket)
        for requested_item
        in pt_release.requestedItems
    ]

    return requested_items


def handle_pickticket_released(topic, msg: Message):
    print(f'Consuming {topic}....')
    try:
        pt_released: PickTicketReleased = pt_release_schema.loads(msg.value())
        print(f'Successfully deserialised pt released message, {pt_released}')

        pickticket = PickTicketById.query.filter_by(pickticket_id=pt_released.pickTicketId).first()
        if pickticket:
            allocations = to_allocations_by_pickticket(pickticket, pt_released)
            requested_items = to_requested_items_by_pickticket(pickticket, pt_released)
            try:
                db.session.add_all(allocations)
                db.session.add_all(requested_items)
                db.session.commit()
                print(f'Added Requested Items and Allocations to Db for PickTicket {pickticket.pickticket_id}')
            except Exception as err:
                print(f'Failed to add Requested Items and Allocations to Db for PickTicket {pickticket.pickticket_id}')
        else:
            print(f'Could not find PickTicket {pt_released.pickTicketId}')
    except Exception as err:
        print(f'failed to deserialise, {err}')
