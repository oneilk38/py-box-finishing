import json
from functools import partial

from flask import Flask, jsonify, request


from common.exceptions import PickTicketNotFoundException, ContainerNotFoundException, InvalidErrorException, \
    UnexpectedItemException
from api.pick_error import can_report_errors
from api.pickticket import get_pickticket_dto
from api.pack import can_pack_pickticket
from api.putwall import can_move_to_putwall, can_remove_from_putwall
from common.contracts import pick_errors_schema
from common.producer import produce

from common.database import db
# Tables

from common.tables import \
    PickTicketById, \
    ItemErrorsByPickTicket

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


@app.route('/api/box-finishing/<fcid>/<pickticket_id>/putwall/move', methods=['PUT'])
def move_to_putwall(fcid: str, pickticket_id: str):
    try:
        produce_action = partial(produce, 'broker:29092', worker_topic)
        if can_move_to_putwall(PickTicketById.get_pickticket, produce_action, fcid, pickticket_id):
            return jsonify(f'Successfully moved PickTicket {pickticket_id} to Putwall'), 201
        else:
            return jsonify(f'Failed to move PickTicket {pickticket_id}, invalid state'), 201
    except PickTicketNotFoundException as not_found:
        print(f'Failed to find PickTicket {pickticket_id}')
        return jsonify(error=f'Failed to find PickTicket {pickticket_id}'), 404
    except Exception as err:
        return jsonify(error=f'Unknown Server Error, {err}'), 500


@app.route('/api/box-finishing/<fcid>/<pickticket_id>/putwall/remove', methods=['PUT'])
def remove_from_putwall(fcid: str, pickticket_id: str):
    try:
        produce_action = partial(produce, 'broker:29092', worker_topic)
        if can_remove_from_putwall(PickTicketById.get_pickticket, produce_action, fcid, pickticket_id):
            return jsonify(f'Successfully removed PickTicket {pickticket_id} from Putwall'), 201
        else:
            return jsonify(f'Failed to remove PickTicket {pickticket_id} from Putwall, invalid state'), 201
    except PickTicketNotFoundException as not_found:
        print(f'Failed to find PickTicket {pickticket_id}')
        return jsonify(error=f'Failed to find PickTicket {pickticket_id}'), 404
    except Exception as err:
        return jsonify(error=f'Unknown Server Error, {err}'), 500


@app.route('/api/box-finishing/<fcid>/<pickticket_id>/error', methods=['POST'])
def report_error(fcid: str, pickticket_id: str):
    try:
        errors = pick_errors_schema.loads(json.dumps(request.json))
        produce_action = partial(produce, 'broker:29092', worker_topic)
        if can_report_errors(PickTicketById.get_pickticket, ItemErrorsByPickTicket.get_item_error_by_pickticket, produce_action, fcid, pickticket_id, errors):
            return jsonify(f'Successfully reported errors for PickTicket {pickticket_id}'), 201
        else:
            return jsonify(f'PickTicket {pickticket_id}, invalid state for reporting errors'), 400
    except PickTicketNotFoundException as not_found:
        print(f'Failed to find PickTicket {pickticket_id}')
        return jsonify(error=f'Failed to find PickTicket {pickticket_id}'), 404
    except InvalidErrorException as invalid_err:
        return jsonify(error=f'{invalid_err}'), 400
    except UnexpectedItemException as unexpected:
        return jsonify(error=f'{unexpected}'), 400
    except Exception as err:
        return jsonify(error=f'Unknown Server Error, {err}'), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
