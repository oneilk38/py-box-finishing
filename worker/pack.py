import sys

from functools import partial
sys.path.append('/app')
from producer.producer import produce
from app import db
from Models.tables import PickTicketById, Status
from contracts import Action, Item, Packed, packed_schema


def get_packed_cancelled_items(pickticket: PickTicketById):
    packed_items = pickticket.picked_items
    errors = [] # pickticket.errors
    # calculate picks less errors
    return list(map(lambda x: Item(gtin=x.gtin, quantity=x.quantity), packed_items)), []


def to_packed_message(pickticket: PickTicketById):
    packed_items, cancelled_items = get_packed_cancelled_items(pickticket)
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


def handle_packed(session, get_pt, produce, persist_pt, action: Action):
    print("Retrieved PickTicket for packing!")

    pickticket: PickTicketById = get_pt(action.fcid, action.pickticket_id)
    packed_message = to_packed_message(pickticket)
    persist_pt(pickticket, session)
    produce(packed_schema.dumps(packed_message), packed_message.pickticket_id)


produce_msg = partial(produce, 'broker:29092', 'python-local-topic')

handle = partial(handle_packed, db.session, PickTicketById.get_pickticket, produce_msg, persist_pt)
