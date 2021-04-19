import json
from dataclasses import dataclass, field
import typing
from typing import List

import marshmallow_dataclass
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from marshmallow import Schema

from Exceptions.Exception import PickTicketNotFoundException, ContainerNotFoundException
from api.pickticket import get_pickticket_dto

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
    try:
        dto = get_pickticket_dto(container_id)
        return jsonify(dto), 200
    except PickTicketNotFoundException as not_found:
        print(f'Failed to find PickTicket associated to Container {container_id}')
        return jsonify(error=f'Failed to find PickTicket associated to Container {container_id}'), 404
    except ContainerNotFoundException as not_found:
        print(f'Failed to find Container {container_id}')
        return jsonify(error=f'Failed to find Container {container_id}'), 404
    except Exception as err:
        return jsonify(error="Unknown Server Error"), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
