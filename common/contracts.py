import typing
from dataclasses import field, dataclass
from typing import List

import marshmallow_dataclass
from marshmallow import EXCLUDE, Schema, fields


# Consumer
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


# Pick Ticket Released
@dataclass
class PickItem:
    class Meta:
        unknown = EXCLUDE
    gtin: str = field(metadata={}, default='gtin')
    quantity: int = field(metadata={}, default=1)


PickItemList = typing.List[PickItem]
AllocationMap = typing.Dict[str, PickItemList]


@dataclass
class PickTicketReleased:
    class Meta:
        unknown = EXCLUDE
    pickTicketId: str = field(metadata={}, default='ptId')
    fcId: str = field(metadata={}, default='fcId')
    cutOff: str = field(metadata={}, default='cutofftime')
    priority: int = field(metadata={}, default=1)
    requestedItems: typing.List[PickItem] = field(default_factory=list)
    allocations: AllocationMap = field(default_factory=dict)
    timestamp: str = field(metadata={}, default='timestamp')


pt_release_schema = marshmallow_dataclass.class_schema(PickTicketReleased)()



@dataclass
class Pick:
    class Meta:
        unknown = EXCLUDE
    gtin: str = field(metadata={}, default='gtin')
    quantity: int = field(metadata={}, default=1)

# Pick
@dataclass
class PickComplete:
    pickTicketId: str = field(metadata={}, default='ptId')
    fcId: str = field(metadata={}, default='fcId')
    sheetId: int = field(default=1)
    containerId: str = field(metadata={}, default='containerId')
    orderId: str = field(metadata={}, default='orderId')
    containerType: str = field(metadata={}, default='containerType')
    destination: str = field(metadata={}, default='destination')
    pickingSource: str = field(metadata={}, default='source')
    picks: typing.List[Pick] = field(default_factory=list)


pick_completed_schema = marshmallow_dataclass.class_schema(PickComplete)()


# Action


# Worker
@dataclass
class PickError(Schema):
    class Meta:
        unknown = EXCLUDE
    gtin: str = field(metadata={})
    missing: int = field(default=1)
    damaged: int = field(default=1)
    overage: int = field(default=1)


pick_error_schema = marshmallow_dataclass.class_schema(PickError)()
pick_errors_schema = marshmallow_dataclass.class_schema(PickError)(many=True)


@marshmallow_dataclass.dataclass
class Action:
    class Meta:
        unknown = EXCLUDE
    type: str = field(metadata={}, default='ACTION')
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')
    container_id: str = field(default=None)
    putwall_location: str = field(default=None)
    errors: List[PickError] = field(default_factory=list)


action_schema = marshmallow_dataclass.class_schema(Action)()


# Producer
@marshmallow_dataclass.dataclass
class Item(Schema):
    class Meta:
        unknown = EXCLUDE
    gtin: str = field(metadata={}, default='gtin')
    quantity: str = field(metadata={}, default=1)


item_schema = marshmallow_dataclass.class_schema(Item)()
items_schema = marshmallow_dataclass.class_schema(Item)(many=True)


@marshmallow_dataclass.dataclass
class Packed(Schema):
    class Meta:
        unknown = EXCLUDE
    packed_items: List[Item] = fields.Nested(item_schema, many=True)
    cancelled_items: List[Item] = fields.Nested(item_schema, many=True)
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')
    container_id: str = field(metadata={}, default='33030101')
    previous_lpns: List[str] = field(default_factory=list)


packed_schema = marshmallow_dataclass.class_schema(Packed)()


@marshmallow_dataclass.dataclass
class PackError(Schema):
    class Meta:
        unknown = EXCLUDE
    item_errors: List[Item] = fields.Nested(pick_error_schema, many=True)
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')
    container_id: str = field(metadata={}, default='33030101')


pack_error_schema = marshmallow_dataclass.class_schema(PackError)()


@marshmallow_dataclass.dataclass
class Putwall(Schema):
    class Meta:
        unknown = EXCLUDE
    items: List[Item] = fields.Nested(item_schema, many=True)
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')
    container_id: str = field(metadata={}, default='33030101')
    previous_lpns: List[str] = field(default_factory=list)


putwall_schema = marshmallow_dataclass.class_schema(Putwall)()

# API




