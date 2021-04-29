import sys

from functools import partial
sys.path.append('/app')
from app import db
from Models.tables import PickTicketById, Status
from contracts import Action


def produce_packed(producer, topic, msg, pickticket_id):
    producer.produce(topic, key=pickticket_id, value="")
    producer.poll(1)


def get_packed_cancelled_items(pickticket: PickTicketById):
    packed_items = pickticket.picked_items
    errors = [] # pickticket.errors
    # calculate picks less errors
    return packed_items, []


def handle_packed(session, get_pt, produce, action: Action):
    print("Retrieved PickTicket for packing!")

    pickticket: PickTicketById = get_pt(action.fcid, action.pickticket_id)
    packed_items, cancelled_items = get_packed_cancelled_items(pickticket)

    for item in packed_items:
        print(f'Packed items : {item.gtin}')
    print(f'Cancelled items: {cancelled_items}')


handle = partial(handle_packed, db.session, PickTicketById.get_pickticket, produce_packed)

