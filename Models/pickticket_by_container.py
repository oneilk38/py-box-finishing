from dataclasses import dataclass
from sqlalchemy import UniqueConstraint, String, Column, Integer, Boolean
from flask_sqlalchemy import Model
from Models.database import db


@dataclass
class PickTicketByContainer(db.Model):
    fcid: str
    container_id: str
    pickticket_id: str

    fcid = Column(String(200), primary_key=True)
    container_id = Column(String(200), primary_key=True)
    pickticket_id = Column(String(200))

    UniqueConstraint('fcid', 'container_id', name='container_unique')
