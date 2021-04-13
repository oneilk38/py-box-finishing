import typing
from dataclasses import dataclass, field
from functools import partial

from confluent_kafka import Message

import marshmallow_dataclass
from Models.picked_items_by_pickticket import PickedItemsByPickTicket
from Models.pickticket_by_id import PickTicketById
from Models.pickticket_by_container import PickTicketByContainer
from marshmallow import EXCLUDE
from app import db


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


def to_pickticket_by_container(pick_complete: PickComplete):
    return PickTicketByContainer(fcid=pick_complete.fcId,
                                 container_id=pick_complete.containerId,
                                 pickticket_id=pick_complete.pickTicketId)


def to_picked_item(fcid, pickticket_id, order_id, pick: Pick) -> PickedItemsByPickTicket:
    return PickedItemsByPickTicket(pickticket_id=pickticket_id,
                                   fcid=fcid,
                                   order_id=order_id,
                                   gtin=pick.gtin,
                                   quantity=pick.quantity)


def persist_container(pick_complete: PickComplete):
    pt_by_container: PickTicketByContainer = to_pickticket_by_container(pick_complete)
    try:
        db.session.add(pt_by_container)
        db.session.commit()
    except Exception as err:
        db.session.rollback()
        print(f'Failed to add container {pick_complete.containerId} to DB, {err}')


def persist_pick_completed(pick_complete: PickComplete):
    to_picked = partial(to_picked_item, pick_complete.fcId, pick_complete.pickTicketId, pick_complete.orderId)
    picked_items = map(to_picked, pick_complete.picks)
    try:
        db.session.add_all(picked_items)
        db.session.commit()
        print(f'Successfully added picks to DB for PickTicket {pick_complete.pickTicketId}')
    except Exception as err:
        db.session.rollback()
        print(f'Failed to add picks to DB for PickTicket {pick_complete.pickTicketId}, {err}')


def persist_pickticket(pick_complete: PickComplete):
    pickticket: PickTicketById = PickTicketById.query.filter_by(pickticket_id=pick_complete.pickTicketId,
                                                                fcid=pick_complete.fcId).first()
    if pickticket:
        pickticket.lpn = pick_complete.containerId
        try:
            db.session.commit()
            print(f'Successfully updated PickTicket {pick_complete.pickTicketId}')
        except Exception as err:
            db.session.rollback()
            print(f'Failed to update PickTicket {pick_complete.pickTicketId}, {err}')
    else:
        print(f'Failed to retrieve PickTicket {pick_complete.pickTicketId}')


def handle_pick_completed(topic, msg: Message):
    print(f'Consuming {topic}....')
    try:
        pick_completed: PickComplete = pick_completed_schema.loads(msg.value())
        print(f'Successfully deserialised pick completed message')
        persist_container(pick_completed)
        persist_pickticket(pick_completed)
        persist_pick_completed(pick_completed)
    except Exception as err:
        print(f'failed to deserialise, {err}')

