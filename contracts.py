from dataclasses import field
from typing import List

import marshmallow_dataclass
from marshmallow import EXCLUDE, Schema, fields


@marshmallow_dataclass.dataclass
class Action:
    class Meta:
        unknown = EXCLUDE
    type: str = field(metadata={}, default='ACTION')
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')


action_schema = marshmallow_dataclass.class_schema(Action)()


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
        #fields = ['packed_items']
    packed_items: List[Item] = fields.Nested(item_schema, many=True)
    #packed_items: List[Item] = fields.List(fields.Nested(item_schema, many=True))
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')
    container_id: str = field(metadata={}, default='33030101')

    cancelled_items: List[Item] = field(default_factory=list)
    previous_lpns: List[str] = field(default_factory=list)



packed_schema = marshmallow_dataclass.class_schema(Packed)()
