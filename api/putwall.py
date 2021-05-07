from Models.tables import PickTicketById, Status
from contracts import Action, action_schema


def is_invalid_status(status: Status):
    return (status == Status.created) or \
           (status == Status.packed) or \
           (status == Status.cancelled)


def can_move_to_putwall(get_pt, produce, fcid, pickticket_id):
    pickticket: PickTicketById = get_pt(fcid, pickticket_id)

    if is_invalid_status(pickticket.status):
        return False

    action = Action(fcid=fcid, pickticket_id=pickticket_id, type="MOVE_TO_PUTWALL")
    produce(action_schema.dumps(action), pickticket_id)
    return True


def can_remove_from_putwall(get_pt, produce, fcid, pickticket_id):
    pickticket: PickTicketById = get_pt(fcid, pickticket_id)

    if is_invalid_status(pickticket.status):
        return False

    action = Action(fcid=fcid, pickticket_id=pickticket_id, type="MOVE_TO_PUTWALL")
    produce(action_schema.dumps(action), pickticket_id)
    return True

