import logging


class WoTestFactRepository:
    logger = logging.getLogger(__name__)

    def __init__(self, db_provider):
        self._db_provider = db_provider

    def update_wo_test_fact_falconDB(self, work_order_id, constraint_key, pass_fail_result, prod_color_dim_ky=None):
        self.logger.info(
            f"""update_wo_test_fact_falconDB(): work_order_id = {work_order_id}, constraint_key = 
            {constraint_key}, result = {pass_fail_result}, prod_color_dim_ky = {prod_color_dim_ky}"""
        )

        self._db_provider.update_wo_test_fact_falconDB(
            work_order_id, constraint_key, pass_fail_result, prod_color_dim_ky
        )
