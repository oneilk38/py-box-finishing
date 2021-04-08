from dataclasses import dataclass
import typing
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "mysql://root:root@db/box-finishing"

db = SQLAlchemy(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@dataclass
class PickTicketById(db.Model):
    fcid: str
    pickticket_id: str
    lpn: str
    asn: str
    status: int
    putwall_location: str

    fcid = db.Column(db.String(200), primary_key=True)
    pickticket_id = db.Column(db.String(200), primary_key=True)
    lpn = db.Column(db.String(200), nullable=True)
    asn = db.Column(db.String(200), nullable=True)
    status = db.Column(db.Integer, nullable=False, default=0)
    putwall_location = db.Column(db.String(200), nullable=True)

    UniqueConstraint('fcid', 'pickticket_id', name='pick_ticket_unique')


@dataclass
class OrderItemsByPickTicket(db.Model):
    fcid: str
    pickticket_id: str
    gtin: str
    title: str
    url: str
    hazmat: bool
    fragile: bool

    fcid = db.Column(db.String(200), primary_key=True)
    pickticket_id = db.Column(db.String(200), primary_key=True)
    gtin = db.Column(db.String(200), primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    url = db.Column(db.String(200), nullable=False)
    hazmat = db.Column(db.Boolean, nullable=False)
    fragile = db.Column(db.Boolean, nullable=False)

    UniqueConstraint('fcid', 'pickticket_id', 'gtin', name='pick_ticket_order_item_unique')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
