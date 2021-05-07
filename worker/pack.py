import sys

from functools import partial
from typing import List

sys.path.append('/app')
from producer.producer import produce
from app import db
from Models.tables import PickTicketById, Status, PickedItemsByPickTicket, ItemErrorsByPickTicket
from contracts import Action, Item, Packed, packed_schema


def get_pick_less_shortages(pickticket: PickTicketById, pick: PickedItemsByPickTicket, get_error):
    error_by_gtin = get_error(fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id, gtin=pick.gtin)
    if error_by_gtin:
        return Item(gtin=pick.gtin, quantity=max(0, pick.quantity - (error_by_gtin.missing + error_by_gtin.damaged)))

    return Item(gtin=pick.gtin, quantity=pick.quantity)


def get_packed_cancelled_items(pickticket: PickTicketById, get_error):
    picked_items_less_shortages: List[Item] = \
        [get_pick_less_shortages(pickticket, pick, get_error) for pick in pickticket.picked_items]

    packed_items = []
    cancelled_items = []

    for requested_item in pickticket.requested_items:
        pick_qty: int = next((pick.quantity for pick in picked_items_less_shortages if pick.gtin == requested_item.gtin), 0)
        if pick_qty > 0:
            if pick_qty >= requested_item.quantity:
                packed_items.append(Item(gtin=requested_item.gtin, quantity=requested_item.quantity))
            else:
                packed_items.append(Item(gtin=requested_item.gtin, quantity=pick_qty))
                cancelled_items.append(Item(gtin=requested_item.gtin, quantity=requested_item.quantity - pick_qty))
        else:
            cancelled_items.append(Item(gtin=requested_item.gtin, quantity=requested_item.quantity))

    return packed_items, cancelled_items


def to_packed_message(pickticket: PickTicketById, get_error):
    packed_items, cancelled_items = get_packed_cancelled_items(pickticket, get_error)
    return Packed(pickticket_id=pickticket.pickticket_id,
                  fcid=pickticket.fcid,
                  container_id=pickticket.lpn,
                  packed_items=packed_items,
                  cancelled_items=cancelled_items,
                  previous_lpns=[])


def persist_pt(pickticket: PickTicketById, session):
    if pickticket.status == Status.packed:
        return
    else:
        pickticket.status = Status.packed
        session.commit()
        print(f'Successfully updated PickTicket {pickticket.pickticket_id} status to Packed!')


def handle_packed(session, get_pt, get_err, produce, persist_pt, action: Action):
    print(f"Retrieved PickTicket for packing!, {action}")

    pickticket: PickTicketById = get_pt(action.fcid, action.pickticket_id)
    packed_message = to_packed_message(pickticket, get_err)
    persist_pt(pickticket, session)
    produce(packed_schema.dumps(packed_message), packed_message.pickticket_id)


produce_msg = partial(produce, 'broker:29092', 'python-local-topic')

handle = partial(handle_packed, db.session, PickTicketById.get_pickticket, ItemErrorsByPickTicket.get_item_error_by_pickticket, produce_msg, persist_pt)
