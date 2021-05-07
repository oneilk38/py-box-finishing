import unittest

from common.tables import PickTicketById, Status, OrderItemsByPickTicket
from consumer.pickticket_summary import Info, PickElement, PickTicketSummary, persist_to_db

from unittest.mock import MagicMock
from unittest import mock

from flask_sqlalchemy import SQLAlchemy

from alchemy_mock.mocking import UnifiedAlchemyMagicMock
db_session = UnifiedAlchemyMagicMock()
db_session.add = MagicMock(return_value=None)
db_session.add_all = MagicMock(return_value=None)
db_session.commit = MagicMock(return_value=None)


class TestX(unittest.TestCase):
    def test_add_one(self):
        pick_item = PickElement(gtin="GTIN1",
                               unitQty=1,
                               pickQty=1,
                               productTitle="title",
                               imgUrl="http:img.jpeg",
                               pickWeight=None,
                               unitWeight=None,
                               sellerId=None,
                               unitOfMeasure=None,
                               isORMD=None,
                               requireOutboundPrep=None)

        info = Info(box=None,
                    pickItems=[pick_item],
                    fcid="9610",
                    pickTicketId="9610504A",
                    originalOrderId=None,
                    carrierMethodId=None,
                    shipToName=None,
                    shipToPhone=None,
                    packageAsn="555545405940590495")

        summary = PickTicketSummary(info=info)

        expected_pickticket = PickTicketById(pickticket_id ="9610504A",
                                            fcid="9610",
                                            lpn=None,
                                            asn="555545405940590495",
                                            status = Status.created,
                                            putwall_location=None)


        session = mock.Mock()
        session.add = MagicMock(return_value=None)
        session.add_all = MagicMock(return_value=None)
        session.commit = MagicMock(return_value=None)

        persist_to_db(session, summary)

        pt: PickTicketById = session.add.call_args[0][0]
        self.assertTrue(pt == expected_pickticket)

        pick_items = session.add_all.call_args[0][0]
        expected_order_item = OrderItemsByPickTicket(
                                                     pickticket_id=info.pickTicketId,
                                                     fcid=info.fcid,
                                                     gtin=pick_item.gtin,
                                                     title=pick_item.productTitle,
                                                     url=pick_item.imgUrl,
                                                     hazmat=pick_item.isORMD,
                                                     fragile=pick_item.requireOutboundPrep)
        self.assertTrue(pick_items[0] == expected_order_item)
