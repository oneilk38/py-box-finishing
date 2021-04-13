from dataclasses import dataclass
from sqlalchemy import UniqueConstraint, String, Column, Integer, Boolean
from flask_sqlalchemy import Model
from Models.database import db


@dataclass
class PickTicketById(db.Model):
    fcid: str
    pickticket_id: str
    lpn: str
    asn: str
    status: int
    putwall_location: str

    fcid = Column(String(200), primary_key=True)
    pickticket_id = Column(String(200), primary_key=True)
    lpn = Column(String(200), nullable=True)
    asn = Column(String(200), nullable=True)
    status = Column(Integer, nullable=False, default=0)
    putwall_location = Column(String(200), nullable=True)

    UniqueConstraint('fcid', 'pickticket_id', name='pick_ticket_unique')