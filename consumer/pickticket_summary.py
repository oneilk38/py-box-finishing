import sys

sys.path.append('/app')
from Exceptions.exns import PoisonMessageException

from app import db
from Models.tables import PickTicketById, OrderItemsByPickTicket, Status

import typing
from dataclasses import dataclass, field
from confluent_kafka import Message

import marshmallow_dataclass
from marshmallow import EXCLUDE
from functools import partial


# Pick Ticket Summary
@dataclass
class Box:
    class Meta:
        unknown = EXCLUDE
    length: float = field(metadata={}, default=0.0)
    width: float = field(metadata={}, default=0.0)
    height: float = field(metadata={}, default=0.0)
    label: str = field(metadata={}, default='label')


@dataclass
class PickElement:
    class Meta:
        unknown = EXCLUDE
    gtin: str = field(metadata={}, default='gtin')
    unitQty: int = field(metadata={}, default=0)
    pickQty: int = field(metadata={}, default=0)
    productTitle: str = field(metadata={}, default='title')
    imgUrl: str = field(metadata={}, default='url')
    pickWeight: float = field(metadata={}, default=0.0)
    unitWeight: float = field(metadata={}, default=0.0)
    sellerId: str = field(metadata={}, default='sellerId')
    unitOfMeasure: str = field(metadata={}, default='measure')
    isORMD: bool = field(metadata={}, default=False)
    requireOutboundPrep: bool = field(metadata={}, default=False)


@dataclass
class ShipBox:
    class Meta:
        unknown = EXCLUDE
    box: Box = field(default_factory=dict)
    boxId: str = field(metadata={}, default='boxId')
    isMailer: bool = field(metadata={}, default=False)


@dataclass
class SourceDropshipInfo:
    class Meta:
        unknown = EXCLUDE
    id: str = field(metadata={}, default='id')
    name: str = field(metadata={}, default='name')


@dataclass
class PurchaseOrderInfo:
    class Meta:
        unknown = EXCLUDE
    purchaseOrderNumber: str = field(metadata={}, default='PO_00000000')
    purchaseOrderDate: str = field(metadata={}, default='18:00:00.0000ZZZ')


@dataclass
class Info:
    class Meta:
        unknown = EXCLUDE
    box: ShipBox = field(default_factory=dict)
    pickItems: typing.List[PickElement] = field(default_factory=list)
    fcid: str = field(metadata={}, default='fcId')
    pickTicketId: str = field(metadata={}, default='ptId')
    originalOrderId: str = field(metadata={}, default='ogId')
    carrierMethodId: str = field(metadata={}, default='cmId')
    shipToPhone: str = field(metadata={}, default='0868961034')
    shipToName: str = field(metadata={}, default='KON')
    packageAsn: str = field(metadata={}, default='asn')


@dataclass
class PickTicketSummary:
    class Meta:
        unknown = EXCLUDE
    info: Info = field(default_factory=dict)


pt_summary_schema = marshmallow_dataclass.class_schema(PickTicketSummary)()


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


