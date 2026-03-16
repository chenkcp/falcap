import logging


class PenSlotFactRepository:
    logger = logging.getLogger(__name__)

    def __init__(self, db_provider):
        self._db_provider = db_provider

    def update_delta_e(self, pn_id, die_site_nr, slot_dim_ky, delta_e):
        # self.logger.info(
        #     f"""update_delta_e(): pn_id: {pn_id} die_site_nr: 
        #     {die_site_nr} slot_dim_ky: {slot_dim_ky} delta_e: {delta_e}"""
        # )

        self._db_provider.update_delta_e(pn_id, die_site_nr, slot_dim_ky, delta_e)

    def get_pen_slot_facts(self, work_order_id, slot_type_cd, prod_color, pen_ids):
        return self._db_provider.get_pens_per_slot_type_cd(work_order_id, slot_type_cd, prod_color, pen_ids )
    
    def get_prod_colors(self, work_order_id, slot_type_cd, pen_ids):
        return self._db_provider.get_prod_colors(work_order_id, slot_type_cd, pen_ids)
    
    def get_delta_e_coordinates_for_slot_type_cd(self, work_order_id, slot_type_cd, prod_color, pen_ids):
        return self._db_provider.get_delta_e_coordinates_for_slot_type_cd(work_order_id,  slot_type_cd, prod_color, pen_ids)