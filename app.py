import json
import typing
from functools import partial

from confluent_kafka import Producer
from flask import Flask, jsonify, request


from Exceptions.exns import PickTicketNotFoundException, ContainerNotFoundException
from api.pickticket import get_pickticket_dto
from api.pack import can_pack_pickticket
from producer.producer import produce

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


worker_topic = "pickticket-events"

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


@app.route('/api/box-finishing/<fcid>/<pickticket_id>/pack', methods=['PUT'])
def pack(fcid: str, pickticket_id: str):
    try:
        produce_action = partial(produce, 'broker:29092', worker_topic)
        if can_pack_pickticket(PickTicketById.get_pickticket, produce_action, fcid, pickticket_id):
            return jsonify(f'Successfully packed PickTicket {pickticket_id}'), 201
        else:
            return jsonify(f'Failed to pack PickTicket {pickticket_id}, invalid state for Packing'), 201
    except PickTicketNotFoundException as not_found:
        print(f'Failed to find PickTicket {pickticket_id}')
        return jsonify(error=f'Failed to find PickTicket {pickticket_id}'), 404
    except Exception as err:
        return jsonify(error=f'Unknown Server Error, {err}'), 500



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
