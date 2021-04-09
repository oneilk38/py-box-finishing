from dataclasses import dataclass
from sqlalchemy import UniqueConstraint, String, Column, Integer, Boolean
from flask_sqlalchemy import Model

@dataclass
class OrderItemsByPickTicket(Model):
    fcid: str
    pickticket_id: str
    gtin: str
    title: str
    url: str
    hazmat: bool
    fragile: bool

    fcid = Column(String(200), primary_key=True)
    pickticket_id = Column(String(200), primary_key=True)
    gtin = Column(String(200), primary_key=True)
    title = Column(String(200), nullable=False)
    url = Column(String(200), nullable=False)
    hazmat = Column(Boolean, nullable=False)
    fragile = Column(Boolean, nullable=False)

    UniqueConstraint('fcid', 'pickticket_id', 'gtin', name='pick_ticket_order_item_unique')