import typing, sys
from functools import partial

sys.path.append('/app')
from Exceptions.exns import PoisonMessageException, PickTicketNotFoundException, InvalidPickTicketStateException

from dataclasses import dataclass, field

from confluent_kafka import Message
from app import db
from Models.tables import AllocationsByPickTicket, RequestedItemsByPickTicket, PickTicketById, Status

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


def deserialise(msg: Message):
    try:
        return pt_release_schema.loads(msg.value())
    except Exception as err:
        raise PoisonMessageException(f'Failed to deserialise, Cannot process this message, {msg.value()}, err: {err}')


def persist_released_to_db(session, pt_released: PickTicketReleased, pickticket : PickTicketById):
    allocations = to_allocations_by_pickticket(pickticket, pt_released)
    requested_items = to_requested_items_by_pickticket(pickticket, pt_released)
    session.add_all(allocations)
    session.add_all(requested_items)
    session.commit()
    print(f'Successfully updated PickTicket {pickticket.pickticket_id}, allocations and requested items to DB!')


def persist_cancelled_to_db(session, pt_released: PickTicketReleased, pickticket):
    pickticket.status = Status.cancelled
    session.commit()
    print(f'PickTicket {pickticket.pickticket_id} cancelled!')


def persist_to_db(session, get_pt, persist_released, persist_cancelled, pt_released: PickTicketReleased):
    if pt_released.pickTicketId == None or pt_released.fcId == None:
        raise PoisonMessageException(f'Cannot process this message, invalid fcid/pickticket, {pt_released}')

    pickticket: PickTicketById = get_pt(pt_released.fcId, pt_released.pickTicketId)

    if pickticket.status == Status.created:
        if len(pt_released.requestedItems) == 0: persist_cancelled(session, pt_released, pickticket)
        else: persist_released(session, pt_released, pickticket)
    else:
        raise InvalidPickTicketStateException(f'Invalid state, not processing released message')


def handle_pickticket_released(persist, msg: Message):
    pt_released: PickTicketReleased = deserialise(msg)
    persist(pt_released)


# Partially Apply
persist_released = partial(partial(persist_to_db, db.session),
                           PickTicketById.get_pickticket,
                           persist_released_to_db,
                           persist_cancelled_to_db)
