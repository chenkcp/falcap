class DynamoDBConfig:
    def __init__(
        self,
        region,
        work_order_status_table_name,
        should_update_work_order_status,
        inventory_color_lookup_table_name,
    ):
        self._region = region
        self._work_order_status_table_name = work_order_status_table_name
        self._should_update_work_order_status = should_update_work_order_status
        self._inventory_color_lookup_table_name = inventory_color_lookup_table_name

    @property
    def region(self):
        return self._region

    @property
    def work_order_status_table_name(self):
        return self._work_order_status_table_name

    @property
    def should_update_work_order_status(self):
        return self._should_update_work_order_status

    @property
    def inventory_color_lookup_table_name(self):
        return self._inventory_color_lookup_table_name
