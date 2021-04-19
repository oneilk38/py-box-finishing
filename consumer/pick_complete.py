import typing
from dataclasses import dataclass, field
from functools import partial

from confluent_kafka import Message

import marshmallow_dataclass

from Exceptions.Exception import PickTicketNotFoundException, PoisonMessageException, InvalidPickTicketStateException
from Models.tables import PickedItemsByPickTicket, PickTicketById, PickTicketByContainer, Status

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


def persist_pickticket_to_db(pick_complete: PickComplete, pickticket: PickTicketById):
    pickticket.lpn = pick_complete.containerId
    pickticket.status = Status.picked
    print(f'Successfully updated PickTicket {pickticket.pickticket_id}')


def persist_container_to_db(pick_complete: PickComplete, pickticket: PickTicketById):
    container = to_pickticket_by_container(pick_complete=pick_complete, pickticket=pickticket)
    db.session.add(container)
    print(f'Successfully updated Container {container.container_id}')


def persist_picked_items_to_db(pick_complete: PickComplete, pickticket: PickTicketById):
    picked_items = to_picked_items(pickticket, pick_complete=pick_complete)
    db.session.add_all(picked_items)
    print(f'Successfully saved picked items to DB for PickTicket {pickticket.pickticket_id}')


def validate_message(pick_complete: PickComplete):
    if pick_complete.pickTicketId == None or pick_complete.fcId == None:
        raise PoisonMessageException(f'Cannot process this message, invalid fcid/pickticket, {pick_complete}')

    if pick_complete.containerId == None:
        raise PoisonMessageException(f'Cannot process this message, invalid containerid, {pick_complete}')

    if len(pick_complete.picks) == 0:
        raise PoisonMessageException(f'Cannot process this message, empty picks, {pick_complete}')


def deserialise(msg: Message):
    try:
        return pick_completed_schema.loads(msg.value())
    except Exception as err:
        raise PoisonMessageException(f'Failed to deserialise, Cannot process this message, {msg.value()}, err: {err}')


def persist_to_db(get_pt, persist_pt, persist_container, persist_picked_items, pick_complete: PickComplete):
    validate_message(pick_complete)

    if pick_complete.destination != "BoxFinishing":
        print(f'Ignoring as not for BoxFinishing')
        return

    pickticket = get_pt(pick_complete.fcId, pick_complete.pickTicketId)

    if pickticket.status == Status.created:
        persist_pt(pick_complete, pickticket)
        persist_container(pick_complete, pickticket)
        persist_picked_items(pick_complete, pickticket)
        db.session.commit()
    else:
        raise InvalidPickTicketStateException(f'Already consumed picked message for PickTicket {pickticket.pickticket_id}, ignoring.')


def handle_pick_completed(persist, msg: Message):
    try:
        pick_completed: PickComplete = deserialise(msg)
        persist(pick_completed)
    except PoisonMessageException as poison:
        print(f'Ignoring message as cant process, {poison}')
    except PickTicketNotFoundException as not_found:
        print(f'Pick Ticket does not exist, ignoring released message, {not_found}')
    except InvalidPickTicketStateException as invalid_state:
        print(f'Ignoring message as invalid state, {invalid_state}')
    except Exception as err:
        db.session.rollback()
        print(f'Failed to add to DB, {err}, {msg.value()}')


# Partially Apply
persist_pick_complete = partial(persist_to_db,
                                PickTicketById.get_pickticket,
                                persist_pickticket_to_db,
                                persist_container_to_db,
                                persist_picked_items_to_db)

