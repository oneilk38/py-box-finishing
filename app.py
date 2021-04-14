import json
from dataclasses import dataclass, field
import typing
from typing import List

import marshmallow_dataclass
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from marshmallow import Schema
from api.pickticket import get_pickticket

from Models.database import db
# Tables

from Models.tables import \
    PickTicketById, \
    OrderItemsByPickTicket, \
    PickedItemsByPickTicket, \
    PickTicketByContainer, \
    AllocationsByPickTicket, \
    RequestedItemsByPickTicket


app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "mysql://root:root@db/box-finishing"

db.app = app
db.init_app(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/api/box-finishing/<fcid>/<container_id>', methods=['GET'])
def get(fcid: str, container_id: str):
    query = db.session\
            .query(PickTicketByContainer, PickTicketById)\
            .filter_by(container_id=container_id).join(PickTicketById).first()

    pt: PickTicketById = query[1]

    if pt:
        dto = get_pickticket(pt)
        return jsonify(dto), 200
    else:
        return jsonify(error=f'Could not find PickTicket associated to container {container_id}'), 404


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
