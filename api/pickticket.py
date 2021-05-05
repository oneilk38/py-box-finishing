from dataclasses import dataclass, field
from typing import List

import marshmallow
import marshmallow_dataclass
from marshmallow import Schema, EXCLUDE, fields

# Tables
from Models.tables import \
    RequestedItemsByPickTicket, \
    AllocationsByPickTicket, \
    PickTicketById, \
    OrderItemsByPickTicket, \
    PickedItemsByPickTicket


@dataclass
class PickItem(Schema):
    class Meta:
        unknown = EXCLUDE
    gtin: str = field()
    quantity: int = field()


def to_pick_item(gtin, quantity):
    return PickItem(gtin=gtin, quantity=quantity)


@dataclass
class OrderItem(Schema):
    class Meta:
        unknown = EXCLUDE
    gtin: str = field()
    imageUrl: str = field()
    isHazmat: bool = field()
    isORMD: bool = field()
    title: str = field()


def to_order_item(gtin, imageUrl, isHazmat, isORMD, title):
    return OrderItem(gtin=gtin, imageUrl=imageUrl, isHazmat=isHazmat, isORMD=isORMD, title=title)



@dataclass
class Packed(Schema):
    class Meta:
        unknown = EXCLUDE
        #fields = ['packed_items']
    packed_items: List[Item] = fields.Nested(item_schema, many=True)
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')
    container_id: str = field(metadata={}, default='33030101')

    cancelled_items: List[Item] = field(default_factory=list)
    previous_lpns: List[str] = field(default_factory=list)


def to_pickticket_response(pickticket_by_id: PickTicketById):

    order_items = [to_order_item(item.gtin, item.url, item.hazmat, item.fragile, item.title) for item in pickticket_by_id.order_items]
    allocations = [to_pick_item(allocation.gtin, allocation.quantity) for allocation in pickticket_by_id.allocations]
    picked_items = [to_pick_item(picked_item.gtin, picked_item.quantity) for picked_item in pickticket_by_id.picked_items]
    requested_items = [to_pick_item(requested_item.gtin, requested_item.quantity) for requested_item in pickticket_by_id.requested_items]

    res = {
        'pickTicketId': pickticket_by_id.pickticket_id,
        'fcId': pickticket_by_id.fcid,
        'lpn': pickticket_by_id.lpn,
        'asn': pickticket_by_id.asn,
        'orderItems': order_items,
        'requestedItems': requested_items,
        'pickedItems': picked_items,
        'allocations': allocations,
        'status': pickticket_by_id.status.to_string()
    }

    return res


def get_pickticket_dto(container_id):
    pickticket = PickTicketById.get_pickticket_by_container(container_id)
    dto = to_pickticket_response(pickticket)
    return dto
