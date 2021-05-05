from dataclasses import field
from functools import partial

import marshmallow_dataclass, sys
from confluent_kafka import Producer
from marshmallow import EXCLUDE


from Models.tables import PickTicketById, Status
from producer.producer import produce

sys.path.append('/app')
from contracts import Action, action_schema


def get_action(pickticket: PickTicketById):
    # check if its already been packed
    if pickticket.status == Status.packed:
        return Action(type="PACKED", fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id)
    elif pickticket.status == Status.cancelled:
        return Action(type="CANCELLED", fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id)
    else:
        # otherwise its not been packed, decide based on quantities
        picked_qty = sum([picked_item.quantity for picked_item in pickticket.picked_items])
        print(picked_qty)
        error_qty = 0
        if picked_qty > error_qty:
            return Action(type="PACKED", fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id)
        else:
            return Action(type="CANCELLED", fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id)


def can_pack_pickticket(get_pt, produce, fcid: str, pickticket_id: str):
    pickticket: PickTicketById = get_pt(fcid, pickticket_id)

    if pickticket.status == Status.created:
        return False
    else:
        action = get_action(pickticket)
        produce(action_schema.dumps(action), pickticket.pickticket_id)
        return True

