from dataclasses import dataclass, field
from typing import List, Optional

import marshmallow
import marshmallow_dataclass
from marshmallow import Schema, EXCLUDE

from common.exceptions import InvalidPickTicketStateException, UnexpectedItemException, InvalidErrorException
from common.tables import PickTicketById, Status, RequestedItemsByPickTicket, ItemErrorsByPickTicket
from contracts import Action, action_schema, PickError


def get_existing_error_quantities(existing_error: ItemErrorsByPickTicket):
    if existing_error:
        return existing_error.missing, existing_error.damaged, existing_error.overage
    else:
        return 0, 0, 0


def validate_error(pickticket: PickTicketById, error: PickError, get_err):
    print(error)
    requested_item: Optional[RequestedItemsByPickTicket] = next((requested_item for requested_item in pickticket.requested_items if requested_item.gtin == error.gtin), None)
    if requested_item:
        existing_error = get_err(fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id, gtin=error.gtin)
        missing, damaged, overage = get_existing_error_quantities(existing_error)
        total_shortage = (missing + error.missing) + (damaged + error.damaged)

        if total_shortage > requested_item.quantity:
            raise InvalidErrorException(f'Existing Shortages ({missing + damaged}) + New Shortages ({error.damaged + error.missing}) exceed Requested quantity ({requested_item.quantity}) for Gtin {error.gtin}, PickTicket {pickticket.pickticket_id}')
        return
    raise UnexpectedItemException(f'Cannot mark error for gtin {error.gtin} as it is not a requested item for PickTicket {pickticket.pickticket_id}')


def can_report_errors(get_pt, get_error, produce, fcid, pickticket_id, errors: List[PickError]):
    # get pick ticket, existing_errors
    pickticket: PickTicketById = get_pt(fcid, pickticket_id)

    if (pickticket.status == Status.packed) or (pickticket.status == Status.created):
        return False

    for error in errors:
        validate_error(pickticket, error, get_error)

    action = Action(type="PICK_ERROR", fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id, errors=errors)
    produce(action_schema.dumps(action), pickticket_id)

    return True
