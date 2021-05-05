import json, time

from producer.producer import produce


def acked(err, msg):
    if err is not None:
        print("Failed to produce message")
    else:
        print(f'Message produced : {msg}')


def getSummaryMsg(pickticket_id):
    return json.dumps({
        "info": {
            "fcid": "9610",
            "pickTicketId": pickticket_id,
            "originalOrderId": "BGdfdHP31555",
            "carrierMethodId": "cmId",
            "orderAttributes": {
                "boxCatalog": "j",
                "sourceDropshipInfo": {
                    "id": "n",
                    "name": "J"
                },
                "purchaseOrderInfo": {
                    "purchaseOrderNumber": "rl",
                    "purchaseOrderDate": "2091-02-27T20:12:38.592+09:52"
                },
                "sourceOrderId": "F",
                "sourceOrderNumber": "XR",
                "customerOrderId": "j",
                "sellerId": "P",
                "orderChannelId": "E"
            },
            "pickItems": [
                {
                    "gtin": "GTIN-1",
                    "unitQty": 100,
                    "pickQty": 100,
                    "productTitle": "a",
                    "imgUrl": "n",
                    "pickWeight": 0.6017836013150548366601785366,
                    "unitWeight": 0.6017836013150548366601785366,
                    "sellerId": "A",
                    "unitOfMeasure": "W",
                    "isORMD": False,
                    "unClassification": None,
                    "alcohol": None
                }
            ],
            "box": {
                "boxId": "Z",
                "box": {
                    "length": 0.1297916650923522604323867214,
                    "width": 0.9201300229502467074105885591,
                    "height": 0.4146484825095313245288074108,
                    "label": "O",
                    "numShippingLabels": None
                },
                "isMailer": False,
                "consolidationOnly": None,
                "bagging": None
            },
            "fallbackBox": None,
            "shipToPhone": "",
            "shipToName": "",
            "shipToAddress": {
                "city": "U",
                "zip": "bNu",
                "state": "3",
                "address1": "4",
                "address2": "H",
                "isBusiness": True,
                "isWeekendDeliveries": False,
                "businessName": "0tI"
            },
            "requestDeliveryBy": "2020-06-24T19:36:45.6648491+00:00",
            "signatureRequirement": None,
            "packagingType": None,
            "deliveryInstructions": None,
            "packageAsn": "asn",
            "timestamp": "2020-06-24T19:36:45.6648527+00:00"
        },
        "shipMethod": {
            "shippingCarrier": "carrier",
            "shippingMethod": "method",
            "requiredShipDate": "2020-06-24T19:36:45.6650095+00:00",
            "requestServiceLevel": None
        }
    })


def getReleasedMsg(pickticket_id):
    return json.dumps({
        "pickTicketId": pickticket_id,
        "fcId": "9610",
        "cutOff": "2093-05-12T18:08:17.829-05:11",
        "priority": 0,
        "destination": "Box",
        "allocations": {"asrs": [{"gtin": "GTIN-1", "quantity": 100}]},
        "requestedItems": [{"gtin": "GTIN-1", "quantity": 100}],
        "timestamp": "ts"
    })


def getPickCompleteMsg(pickticket_id, container_id):
    return json.dumps({
        'containerId': container_id,
        'pickTicketId': pickticket_id,
        'sheetId': 1,
        'orderId': "9610435453_1",
        'fcId': "9610",
        'containerType': "Tote",
        'destination': "BoxFinishing",
        'pickingSource': "ASRS",
        'picks': [
            {
              "gtin": "GTIN-1",
              "quantity": 100
            }
        ]
    })


def run_summary_producer(pickticket_id):
    produce(broker='localhost:9092', topic="pt-summary", key="msg1", json=getSummaryMsg(pickticket_id))


def run_release_producer(pickticket_id):
    produce(broker='localhost:9092', topic="pt-release", key="msg1", json=getReleasedMsg(pickticket_id))


def run_complete_producer(pickticket_id, container_id):
    produce(broker='localhost:9092', topic="pick-completed", key="msg1", json=getPickCompleteMsg(pickticket_id, container_id))


pickticket_id = "96203503AAX"
container_id = "409549"

run_summary_producer(pickticket_id)
time.sleep(2.0)
run_release_producer(pickticket_id)
time.sleep(2.0)
run_complete_producer(pickticket_id, container_id)
