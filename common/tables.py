from dataclasses import dataclass
from re import match

from sqlalchemy import UniqueConstraint, String, Column, Integer, Boolean, CheckConstraint, Enum

from common.exceptions import PickTicketNotFoundException, ContainerNotFoundException
from common.database import db
import enum


class Status(enum.Enum):
    created = 1
    picked = 2
    packed = 3
    error = 4
    putwall = 5
    cancelled = 6

    def to_string(self):
        if self == Status.created: return "Created"
        elif self == Status.picked: return "Picked"
        elif self == Status.packed: return "Packed"
        elif self == Status.putwall: return "Putwall"
        elif self == Status.cancelled: return "Cancelled"
        else: return "Error"


@dataclass
class PickTicketById(db.Model):
    __tablename__ = "pickticket"
    pickticket_id = Column(String(200), primary_key=True)
    fcid = Column(String(200))
    lpn = Column(String(200), nullable=True)
    asn = Column(String(200), nullable=True)
    status = Column(Enum(Status))
    # Column(Integer, nullable=False, default=0)
    putwall_location = Column(String(200), nullable=True)

    order_items = db.relationship('OrderItemsByPickTicket', backref='pickticket')
    picked_items = db.relationship('PickedItemsByPickTicket', backref='pickticket')
    allocations = db.relationship('AllocationsByPickTicket', backref='pickticket')
    requested_items = db.relationship('RequestedItemsByPickTicket', backref='pickticket')
    container = db.relationship('PickTicketByContainer', backref='pickticket')
    errors = db.relationship('ItemErrorsByPickTicket', backref='pickticket')

    def __eq__(self, other):
        return  (self.pickticket_id == other.pickticket_id) and \
                (self.fcid == other.fcid) and \
                (self.lpn == other.lpn) and \
                (self.asn == other.asn) and \
                (self.status == other.status) and \
                (self.putwall_location == other.putwall_location)



    #__table_args = (
    #    CheckConstraint(status >= 0 and status <= 5, name='check_valid_enum'),
    #    {})

    #UniqueConstraint('fcid', 'pickticket_id', name='pick_ticket_unique')

    @staticmethod
    def get_pickticket(fcid, pickticket_id):
        pickticket: PickTicketById = PickTicketById.query.filter_by(pickticket_id=pickticket_id, fcid=fcid).first()
        if pickticket:
            return pickticket
        else:
            raise PickTicketNotFoundException(f'Could not find PickTicket {pickticket_id}')


    @staticmethod
    def get_pickticket_by_container(container_id):
        container: PickTicketByContainer = PickTicketByContainer.get_container(container_id)

        query = db.session \
            .query(PickTicketByContainer, PickTicketById) \
            .filter_by(container_id=container.container_id).join(PickTicketById).first()

        # Query returns Container, PickTicket object as a tuple
        pickticket: PickTicketById = query[1]

        if pickticket:
            return pickticket
        else:
            raise PickTicketNotFoundException(f'Could not find PickTicket associated to Container {container_id}')



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

    def __eq__(self, other):
        return  (self.pickticket_id == other.pickticket_id) and \
                (self.fcid == other.fcid) and \
                (self.gtin == other.gtin) and \
                (self.title == other.title) and \
                (self.url == other.url) and \
                (self.hazmat == other.hazmat) and \
                (self.fragile == other.fragile)


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
class ItemErrorsByPickTicket(db.Model):
    __tablename__ = "item-error"
    id = Column(Integer, primary_key=True)
    pickticket_id = Column(String(200), db.ForeignKey(PickTicketById.pickticket_id))
    fcid = Column(String(200))
    gtin = Column(String(200))
    missing = Column(Integer, nullable=False, default=1)
    damaged = Column(Integer, nullable=False, default=1)
    overage = Column(Integer, nullable=False, default=1)

    @staticmethod
    def get_item_error_by_pickticket(fcid, pickticket_id, gtin):
        error = ItemErrorsByPickTicket.query.filter_by(pickticket_id=pickticket_id, fcid=fcid, gtin=gtin).first()
        return error

    @staticmethod
    def get_item_errors_by_pickticket(fcid, pickticket_id):
        errors = ItemErrorsByPickTicket.query.filter_by(pickticket_id=pickticket_id, fcid=fcid)
        return errors

    # UniqueConstraint('fcid', 'pickticket_id', 'gtin', name='item_error_unique')


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

    @staticmethod
    def get_container(container_id):
        container = PickTicketByContainer.query.filter_by(container_id=container_id).first()
        if container:
            return container
        else:
            raise ContainerNotFoundException(f'Could not find Container {container_id}')

    #UniqueConstraint('fcid', 'container_id', name='container_unique')