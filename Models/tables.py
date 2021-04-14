from dataclasses import dataclass
from sqlalchemy import UniqueConstraint, String, Column, Integer, Boolean
from flask_sqlalchemy import Model
from Models.database import db


@dataclass
class PickTicketById(db.Model):
    __tablename__ = "pickticket"
    pickticket_id = Column(String(200), primary_key=True)
    fcid = Column(String(200))
    lpn = Column(String(200), nullable=True)
    asn = Column(String(200), nullable=True)
    status = Column(Integer, nullable=False, default=0)
    putwall_location = Column(String(200), nullable=True)

    order_items = db.relationship('OrderItemsByPickTicket', backref='pickticket')
    picked_items = db.relationship('PickedItemsByPickTicket', backref='pickticket')
    allocations = db.relationship('AllocationsByPickTicket', backref='pickticket')
    requested_items = db.relationship('RequestedItemsByPickTicket', backref='pickticket')
    container = db.relationship('PickTicketByContainer', backref='pickticket')
    #UniqueConstraint('fcid', 'pickticket_id', name='pick_ticket_unique')


@dataclass
class OrderItemsByPickTicket(db.Model):
    __tablename__ = "order-item"
    id = Column(Integer, primary_key=True)
    pickticket_id = Column(String(200), db.ForeignKey(PickTicketById.pickticket_id))
    fcid = Column(String(200))
    gtin = Column(String(200))
    title = Column(String(200), nullable=False)
    url = Column(String(200), nullable=False)
    hazmat = Column(Boolean, nullable=False)
    fragile = Column(Boolean, nullable=False)

    # UniqueConstraint('fcid', 'pickticket_id', 'gtin', name='pick_ticket_order_item_unique')


@dataclass
class RequestedItemsByPickTicket(db.Model):
    __tablename__ = "requested-item"
    id = Column(Integer, primary_key=True)
    pickticket_id = Column(String(200), db.ForeignKey(PickTicketById.pickticket_id))
    fcid = Column(String(200))
    gtin = Column(String(200))
    quantity = Column(Integer, nullable=False, default=1)

    # UniqueConstraint('fcid', 'pickticket_id', 'gtin', name='requested_item_unique')


@dataclass
class AllocationsByPickTicket(db.Model):
    __tablename__ = "allocation"
    id = Column(Integer, primary_key=True)
    pickticket_id = Column(String(200), db.ForeignKey(PickTicketById.pickticket_id))
    fcid = Column(String(200))
    gtin = Column(String(200))
    quantity = Column(Integer, nullable=False, default=1)

    # UniqueConstraint('fcid', 'pickticket_id', 'gtin', name='allocation_item_unique')


@dataclass
class PickedItemsByPickTicket(db.Model):
    __tablename__ = "picked-item"
    id = Column(Integer, primary_key=True)
    pickticket_id = Column(String(200), db.ForeignKey(PickTicketById.pickticket_id))
    fcid = Column(String(200))
    gtin = Column(String(200))
    quantity = Column(Integer, nullable=False, default=1)

    #UniqueConstraint('fcid', 'pickticket_id', 'order_id', 'gtin', name='picked_item_unique')


@dataclass
class PickTicketByContainer(db.Model):
    __tablename__ = "pickticket-by-container"
    fcid = Column(String(200), primary_key=True)
    container_id = Column(String(200), primary_key=True)
    pickticket_id = Column(String(200), db.ForeignKey(PickTicketById.pickticket_id))

    #UniqueConstraint('fcid', 'container_id', name='container_unique')