from dataclasses import dataclass
from sqlalchemy import UniqueConstraint, String, Column, Integer, Boolean
from flask_sqlalchemy import Model

@dataclass
class AllocationsByPickTicket(Model):
    fcid: str
    pickticket_id: str
    gtin: str
    quantity: int

    fcid = Column(String(200), primary_key=True)
    pickticket_id = Column(String(200), primary_key=True)
    gtin = Column(String(200), primary_key=True)
    quantity = Column(Integer, nullable=False, default=1)
