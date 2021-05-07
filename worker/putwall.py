from functools import partial
from typing import List

from common.exceptions import InvalidPickTicketStateException
from common.tables import PickTicketById, Status, PickedItemsByPickTicket, ItemErrorsByPickTicket
from common.contracts import putwall_schema, Item, Putwall
from producer.producer import produce

import sys

sys.path.append('/app')

from app import db


def get_pick_less_shortages(pick: PickedItemsByPickTicket, errors: List[ItemErrorsByPickTicket]):
    error_qty = next(
        ((error.missing + error.damaged)
         for error in errors
         if error.gtin == pick.gtin),
        0)

    return Item(gtin=pick.gtin, quantity=max(0, pick.quantity - error_qty))


def get_items_less_shortages(pickticket: PickTicketById):
    picks_less_shortages = [
        get_pick_less_shortages(pick, pickticket.errors)
        for pick in pickticket.picked_items
    ]

    return picks_less_shortages


def to_putwall_message(pickticket: PickTicketById):
    items = get_items_less_shortages(pickticket)

    return Putwall(fcid=pickticket.fcid,
                   pickticket_id=pickticket.pickticket_id,
                   container_id=pickticket.lpn,
                   items=items,
                   previous_lpns=[])


def persist_move(session, pickticket: PickTicketById):
    if not pickticket.status == Status.putwall:
        pickticket.status = Status.putwall
        session.commit()
        print(f'Successfully moved PickTicket {pickticket.pickticket_id} to Putwall!')


def persist_remove(session, pickticket: PickTicketById):
    if pickticket.status == Status.putwall:
        pickticket.status = Status.picked
        session.commit()
        print(f'Successfully removed PickTicket {pickticket.pickticket_id} from Putwall!')


def handle(session, get_pt, produce_action, persist, action):
    pickticket: PickTicketById = get_pt(action.fcid, action.pickticket_id)

    if pickticket.status == Status.packed:
        raise InvalidPickTicketStateException(f'PickTicket {pickticket.pickticket_id} is already Packed!')

    persist(session, pickticket)

    putwall_msg = to_putwall_message(pickticket)
    produce_action(putwall_schema.dumps(putwall_msg), pickticket.pickticket_id)


produce_msg = partial(produce, 'broker:29092', 'python-local-topic')

handle_moved = partial(handle, db.session, PickTicketById.get_pickticket, produce_msg, persist_move)
handle_removed = partial(handle, db.session, PickTicketById.get_pickticket, produce_msg, persist_remove)

