from dataclasses import dataclass, field
from typing import List

from marshmallow import Schema

# Tables
from Models.requested_items_by_pickticket import RequestedItemsByPickTicket
from Models.allocations_by_pickticket import AllocationsByPickTicket
from Models.pickticket_by_id import PickTicketById
from Models.order_items_by_pickticket import OrderItemsByPickTicket
from Models.picked_items_by_pickticket import PickedItemsByPickTicket


@dataclass
class PickItem(Schema):
    gtin: str = field()
    quantity: int = field()


def to_pick_item(gtin, quantity):
    return PickItem(gtin=gtin, quantity=quantity)


@dataclass
class OrderItem(Schema):
    gtin: str = field()
    imageUrl: str = field()
    isHazmat: bool = field()
    isORMD: bool = field()
    title: str = field()


def to_order_item(gtin, imageUrl, isHazmat, isORMD, title):
    return OrderItem(gtin=gtin, imageUrl=imageUrl, isHazmat=isHazmat, isORMD=isORMD, title=title)


@dataclass
class PickTicket:
    pickticket_id: str = field()
    fcid: str = field()
    lpn: str = field()
    asn: str = field()
    requested_items: List[PickItem] = field(default_factory=list)
    order_items: List[OrderItem] = field(default_factory=list)
    allocations: List[PickItem] = field(default_factory=list)
    picked_items: List[PickItem] = field(default_factory=list)


def to_pickticket_response(pickticket_by_id: PickTicketById,
                  allocations_by_pt: List[AllocationsByPickTicket],
                  requested_items_by_pt: List[RequestedItemsByPickTicket],
                  picked_items_by_pt: List[PickedItemsByPickTicket],
                  order_items_by_pt: List[OrderItemsByPickTicket]):

    order_items = [to_order_item(item.gtin, item.url, item.hazmat, item.fragile, item.title) for item in order_items_by_pt]
    allocations = [to_pick_item(allocation.gtin, allocation.quantity) for allocation in allocations_by_pt]
    picked_items = [to_pick_item(picked_item.gtin, picked_item.quantity) for picked_item in picked_items_by_pt]
    requested_items = [to_pick_item(requested_item.gtin, requested_item.quantity) for requested_item in requested_items_by_pt]

    res = {
        'pickTicketId': pickticket_by_id.pickticket_id,
        'fcId': pickticket_by_id.fcid,
        'lpn': pickticket_by_id.lpn,
        'asn': pickticket_by_id.asn,
        'orderItems': order_items,
        'requestedItems': requested_items,
        'pickedItems': picked_items,
        'allocations': allocations
    }

    return res


def get_pickticket(fcid: str, pickticket_id: str):
    pickticket: PickTicketById = PickTicketById.query.filter_by(pickticket_id=pickticket_id, fcid=fcid).first()
    if pickticket:
        try:
            allocations: List[AllocationsByPickTicket] = AllocationsByPickTicket.query.filter_by(pickticket_id=pickticket_id, fcid=fcid).all()
            requested_items: List[RequestedItemsByPickTicket] = RequestedItemsByPickTicket.query.filter_by(pickticket_id=pickticket_id, fcid=fcid).all()
            order_items: List[OrderItemsByPickTicket] = OrderItemsByPickTicket.query.filter_by(pickticket_id=pickticket_id, fcid=fcid).all()
            picked_items: List[PickedItemsByPickTicket] = PickedItemsByPickTicket.query.filter_by(pickticket_id=pickticket_id, fcid=fcid).all()

            dto = to_pickticket_response(pickticket, allocations, requested_items, picked_items, order_items)

            return dto, 200
        except Exception as err:
            return {'error': err}, 500
    else:
        return {'message': f'Failed to retrieve PickTicket {pickticket_id}'}, 404
