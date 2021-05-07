import sys

sys.path.append('/app')
from common.exceptions import PoisonMessageException

from app import db
from common.tables import PickTicketById, OrderItemsByPickTicket, Status

from confluent_kafka import Message

from functools import partial
from common.contracts import PickTicketSummary, PickElement, pt_summary_schema


def to_pickticket_by_id(summary: PickTicketSummary) -> PickTicketById:
    return PickTicketById(pickticket_id=summary.info.pickTicketId,
                          fcid=summary.info.fcid,
                          asn=summary.info.packageAsn, lpn=None,
                          putwall_location=None,
                          status=Status.created)


def to_order_item_by_pickticket(pickticket_id, fcid, pick_element: PickElement, pickticket: PickTicketById) -> OrderItemsByPickTicket:
    return OrderItemsByPickTicket(pickticket_id=pickticket_id,
                                  fcid=fcid,
                                  gtin=pick_element.gtin,
                                  title=pick_element.productTitle,
                                  url=pick_element.imgUrl,
                                  hazmat=pick_element.isORMD,
                                  fragile=pick_element.requireOutboundPrep,
                                  pickticket=pickticket)


def to_order_items_by_pickticket(pickticket: PickTicketById, pt_summary: PickTicketSummary):
    order_items_by_pickticket = [
        to_order_item_by_pickticket(pt_summary.info.pickTicketId, pt_summary.info.fcid, order_item, pickticket)
        for order_item in pt_summary.info.pickItems
    ]

    return order_items_by_pickticket


def persist_to_db(session, pt_summary: PickTicketSummary):
    if pt_summary.info.pickTicketId == None or len(pt_summary.info.pickItems) == 0:
        raise PoisonMessageException(f'Cannot process this message, {pt_summary}')

    pickticket = to_pickticket_by_id(pt_summary)
    session.add(pickticket)
    order_items_by_pickticket = to_order_items_by_pickticket(pickticket, pt_summary)
    session.add_all(order_items_by_pickticket)
    session.commit()
    print(f'Successfully created PickTicket and order items for PT {pickticket.pickticket_id}')


def deserialise(msg: Message):
    try:
        return pt_summary_schema.loads(msg.value())
    except Exception as err:
        raise PoisonMessageException(f'Failed to deserialise, Cannot process this message, {msg.value()}, err: {err}')


def handle_pickticket_summary(persist, msg: Message):
    try:
        pt_summary: PickTicketSummary = deserialise(msg)
        persist(pt_summary)
    except PoisonMessageException as poison:
        print(f'Ignoring message as cant process, {poison}')
    except Exception as err:
        db.session.rollback()
        print(f'Failed to add to DB, {err}, {msg.value()}')


persist_summary = partial(persist_to_db, db.session)


