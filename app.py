from dataclasses import dataclass, field
import typing
from typing import List

import marshmallow_dataclass
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from marshmallow import Schema

from Models.database import db
# Tables
from Models.requested_items_by_pickticket import RequestedItemsByPickTicket
from Models.allocations_by_pickticket import AllocationsByPickTicket
from Models.pickticket_by_id import PickTicketById
from Models.order_items_by_pickticket import OrderItemsByPickTicket
from Models.picked_items_by_pickticket import PickedItemsByPickTicket
from Models.pickticket_by_container import PickTicketByContainer

from api.pickticket import get_pickticket

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "mysql://root:root@db/box-finishing"

db.app = app
db.init_app(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/api/box-finishing/<fcid>/<container_id>', methods=['GET'])
def get(fcid: str, container_id: str):
    container: PickTicketByContainer = PickTicketByContainer.query.filter_by(fcid=fcid, container_id=container_id).first()
    if container:
        try:
            dto, status_code = get_pickticket(fcid, container.pickticket_id)
            return jsonify(dto), status_code
        except Exception as err:
            print(f'Failed to retrieve PickTicket {container.pickticket_id}, {err}'), 500
    else:
        return jsonify(error=f'Container {container_id} not found'), 404


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
