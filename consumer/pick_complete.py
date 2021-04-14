import typing
from dataclasses import dataclass, field

from confluent_kafka import Message

import marshmallow_dataclass
from Models.tables import PickedItemsByPickTicket, PickTicketById, PickTicketByContainer

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


def to_pickticket_by_container(pick_complete: PickComplete, pickticket: PickTicketById):
    return PickTicketByContainer(fcid=pickticket.fcid,
                                 container_id=pick_complete.containerId,
                                 pickticket_id=pickticket.pickticket_id,
                                 pickticket=pickticket)


def to_picked_item(pick: Pick, pickticket: PickTicketById) -> PickedItemsByPickTicket:
    return PickedItemsByPickTicket(pickticket_id=pickticket.pickticket_id,
                                   fcid=pickticket.fcid,
                                   gtin=pick.gtin,
                                   quantity=pick.quantity)


def to_picked_items(pickticket: PickTicketById, pick_complete: PickComplete):
    picked_items = [
        to_picked_item(picked_item, pickticket)
        for picked_item
        in pick_complete.picks
    ]

    return picked_items


def handle_pick_completed(topic, msg: Message):
    print(f'Consuming {topic}....')
    try:

        pick_completed: PickComplete = pick_completed_schema.loads(msg.value())
        print(f'Successfully deserialised pick completed message')

        pickticket: PickTicketById = PickTicketById.query.filter_by(pickticket_id=pick_completed.pickTicketId).first()
        if pickticket:
            pickticket.lpn = pick_completed.containerId
            container = to_pickticket_by_container(pick_complete=pick_completed, pickticket=pickticket)
            picked_items = to_picked_items(pickticket, pick_complete=pick_completed)
            try:
                db.session.add(container)
                db.session.add_all(picked_items)
                db.session.commit()
                print(f'Successfully added picks to DB for PickTicket {pickticket.pickticket_id}')
            except Exception as err:
                db.session.rollback()
                print(f'Failed to add picks to DB for PickTicket {pickticket.pickticket_id}, {err}')
        else:
            print(f'Could not find PickTicket {pick_completed.pickTicketId}')
    except Exception as err:
        print(f'failed to deserialise, {err}')

