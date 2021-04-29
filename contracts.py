from dataclasses import field

import marshmallow_dataclass
from marshmallow import EXCLUDE


@marshmallow_dataclass.dataclass
class Action:
    class Meta:
        unknown = EXCLUDE
    type: str = field(metadata={}, default='ACTION')
    fcid: str = field(metadata={}, default='fcid')
    pickticket_id: str = field(metadata={}, default='pickticketid')


action_schema = marshmallow_dataclass.class_schema(Action)()