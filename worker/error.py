import sys
from functools import partial
from typing import List

from common.tables import ItemErrorsByPickTicket, PickTicketById
from common.contracts import Action, PickError, PackError, pack_error_schema
from producer.producer import produce

sys.path.append('/app')

from app import db


def persist_error(session, pickticket: PickTicketById, get_error, error: PickError):
    existing_error: ItemErrorsByPickTicket = get_error(fcid=pickticket.fcid, pickticket_id=pickticket.pickticket_id, gtin=error.gtin)
    if existing_error:
        existing_error.missing = existing_error.missing + error.missing
        existing_error.damaged = existing_error.damaged + error.damaged
        existing_error.overage = existing_error.overage + error.overage
        print(f'Updated Error for Gtin {error.gtin}, PickTicket {pickticket.pickticket_id}')
    else:
        error = ItemErrorsByPickTicket(fcid=pickticket.fcid,
                                       pickticket_id=pickticket.pickticket_id,
                                       gtin=error.gtin,
                                       missing=error.missing,
                                       damaged=error.damaged,
                                       overage=error.overage,
                                       pickticket=pickticket)
        session.add(error)
        print(f'Added new error for Gtin {error.gtin}, PickTicket {pickticket.pickticket_id}')


def to_pack_error_message(pickticket: PickTicketById, errors: List[PickError]):
    return PackError(fcid=pickticket.fcid,
                     pickticket_id=pickticket.pickticket_id,
                     container_id=pickticket.lpn,
                     item_errors=errors)


def report_error(session, get_pt, get_error, produce, action: Action):
    pickticket = PickTicketById.get_pickticket(fcid=action.fcid, pickticket_id=action.pickticket_id)

    pack_error_message = to_pack_error_message(pickticket, action.errors)

    produce(pack_error_schema.dumps(pack_error_message), pickticket.pickticket_id)

    for error in action.errors:
        persist_error(session, pickticket, get_error, error)

    session.commit()

    print(f'Successfully saved item errors to DB for PickTicket {pickticket.pickticket_id}')


produce_msg = partial(produce, 'broker:29092', 'python-local-topic')

handle = partial(report_error, db.session, PickTicketById.get_pickticket, ItemErrorsByPickTicket.get_item_error_by_pickticket, produce_msg)