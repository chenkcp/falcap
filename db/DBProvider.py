from abc import ABC, abstractmethod
from .QueryAdapter import QueryAdapter, UniversalQuery


class DBProvider(ABC):
    
    def execute_universal_query(self, cursor, universal_query: UniversalQuery, **params):
        """
        Execute a universal query on this database provider with automatic parameter conversion.
        Must be implemented by subclasses to handle their specific parameter format.
        """
        raise NotImplementedError("Subclasses must implement execute_universal_query")
    
    @abstractmethod
    def set_database_operations(self, should_update, should_insert):
        pass

    @abstractmethod
    def init_connection(self):
        pass

    @abstractmethod
    def is_connected(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass

    @abstractmethod
    def get_pool(self):
        pass

    @abstractmethod
    def get_all_test_types(self):
        pass

    @abstractmethod
    def get_constraints(self, test_type_key):
        pass

    @abstractmethod
    def get_test_criteria(self, constraint_key):
        pass

    @abstractmethod
    def get_all_work_orders(self, filters):
        pass


    @abstractmethod
    def get_work_order(self, work_order_id):
        pass

    @abstractmethod
    def get_printed_count_for_falcap(self, work_order_id):
        pass

    @abstractmethod
    def get_ink_type_dim_ky(self, work_order_id):
        pass

    @abstractmethod
    def get_arch_id(self, work_order_id):
        pass

    @abstractmethod
    def get_prod_colors(self, work_order_id):
        pass

    @abstractmethod
    def get_pen_count(self, work_order_id, inv_item_ky, filters):
        pass

    @abstractmethod
    def get_test_parametric_data(
        self,
        work_order_id,
        table_name,
        column_name,
        slot_type_cd,
        prod_color_dim_ky,
        filters_prod_colors,
    ):
        pass

    @abstractmethod
    def get_dates_to_failure(self, work_order_id, days_to_process_wo_ct):
        pass

    @abstractmethod
    def block_work_orders(self, work_orders):
        pass

    @abstractmethod
    def set_email_sent(self, work_orders):
        pass


    @abstractmethod
    def get_delta_e_coordinates_for_slot_type_cd(
        self, coordinate_key, work_order_id, prod_color_dim_ky, slot_type_cd
    ):
        pass

    @abstractmethod
    def get_pens_per_slot_type_cd(self, work_order_id, slot_type_cd):
        pass

    @abstractmethod
    def get_test_types(self, arch_id, ink_type_dim_ky):
        pass

    @abstractmethod
    def update_delta_e(self, pn_id, die_site_nr, slot_dim_ky, delta_e):
        pass

    @abstractmethod
    def calc_percentile_using_stored_function(self, data, pct):
        pass

    @abstractmethod
    def store_wo_result_falconDB(self, work_order_id, result):
        pass

    @abstractmethod
    def get_failure_reasons(self, work_order_id):
        pass

    @abstractmethod
    def get_work_orders_with_no_email_sent(self, tested_work_order_ids):
        pass

    @abstractmethod
    def get_color_keys(self, prod_color_nm):
        pass