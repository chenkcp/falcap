import logging

from models.TestType import TestType
from models.WorkOrder import WorkOrder


class WoResultFactRepository:
    logger = logging.getLogger(__name__)

    def __init__(self, db_provider):
        self._db_provider = db_provider

    def block_work_orders(self, work_orders):
        self.logger.info("block_work_orders()")
        self._db_provider.block_work_orders(work_orders)

    def set_email_sent(self, work_orders):
        self.logger.info("set_email_sent()")
        self._db_provider.set_email_sent(work_orders)

    def store_wo_result_falconDB(self, work_order_id, result):
        self.logger.info(
            f"store_wo_result_falconDB(): work_order_id: {work_order_id}, result: {result}"
        )
        self._db_provider.store_wo_result_falconDB(work_order_id, result)

    def get_work_orders_with_no_email_sent(self, tested_work_order_ids):
        self.logger.info("get_work_orders_with_no_email_sent()")
        data = self._db_provider.get_work_orders_with_no_email_sent(
            tested_work_order_ids
        )
        test_type = TestType(None, None, None, None, -1, -1, None, None)
        return [
            (WorkOrder(work_order_id, None, None, None, test_type, 1000, None), status)
            for work_order_id, status in data
        ]
