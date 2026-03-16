import logging
import sys
import os
import oracledb

# Handle both relative and absolute imports
try:
    from .DBProvider import DBProvider
    from .QueryAdapter import SharedQueries
except ImportError:
    # If relative import fails, add parent directory to path and try absolute import
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    from db.DBProvider import DBProvider
    from db.QueryAdapter import SharedQueries

class OracleProvider(DBProvider):
    logger = logging.getLogger(__name__)
    _pool = None

    def __init__(
        self,
        user,
        password,
        host,
        port,
        service_name,
        schema,
        should_update=False,
        should_insert=False,
    ):
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._service_name = service_name
        self._schema = schema
        self._should_update = should_update
        self._should_insert = should_insert

    def execute_universal_query(self, cursor, universal_query, **params):
        """Execute a universal query with Oracle parameter format.""" 
        from .QueryAdapter import UniversalQuery
        if isinstance(universal_query, UniversalQuery):
            cursor.execute(universal_query.oracle_query, **params)
        else:
            # Handle string queries directly (assume Oracle format)
            cursor.execute(universal_query, **params)
        return cursor.fetchall()

    def execute_universal_query_one(self, cursor, universal_query, **params):
        """Execute a universal query and return single row with Oracle parameter format."""
        from .QueryAdapter import UniversalQuery
        if isinstance(universal_query, UniversalQuery):
            cursor.execute(universal_query.oracle_query, **params)
        else:
            # Handle string queries directly (assume Oracle format)
            cursor.execute(universal_query, **params)
        return cursor.fetchone()

    def init_connection(self):
        self.logger.info("init_connection()")

        self._pool = oracledb.create_pool(
            user=self._user,
            password=self._password,
            dsn=oracledb.makedsn(
                host=self._host, port=self._port, service_name=self._service_name
            ),
            min=1,
            max=5,
            increment=1,
        )
        connection = self._pool.acquire()
        connection.close()

    def is_connected(self):
        self.logger.info("is_connected()")
        try:
            with self.get_pool().acquire() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM DUAL")
                    return True
        except Exception as e:
            self.logger.error(f"OracleProvider.is_connected() exception: {e}")
            return False

    def close_connection(self):
        self.logger.info("close_connection()")
        self._pool.close()

    def set_database_operations(self, should_update, should_insert):
        self._should_update = should_update
        self._should_insert = should_insert

    def get_all_test_types(self):
        self.logger.info("get_all_test_types()")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT FCEOLQT_WO_TYPE_DIM_KY, WORK_ORDER_TYPE_NM, INK_TYPE_DIM_KY, ARCH_ID, MIN_PEN_CT, 
                DAYS_TO_PROCESS_WO_CT, ACTIVE_FG 
                FROM {self._schema}.FCEOLQT_WO_TYPE_DIM
                WHERE ACTIVE_FG = 'Y'
                """
                self.logger.info(query)
                cursor.execute(query)
                return cursor.fetchall()

    def get_constraints(self, test_type_key):
        self.logger.info(f"get_constraints() for test type key: {test_type_key}")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT FCEOLQT_WO_TEST_CNSTR_DIM_KY, FCEOLQT_TEST_CRITERIA_DIM_KY, PROD_COLOR_DIM_KY, 
                CONSTRAINT_UPPER_BOUND_VL, CONSTRAINT_LOWER_BOUND_VL, CONSTRAINT_CENTILE_PCT,  SLOT_TYPE_CD, ACTIVE_FG 
                FROM {self._schema}.FCEOLQT_WO_TEST_CNSTR_DIM
                WHERE FCEOLQT_WO_TYPE_DIM_KY = :test_type_key AND ACTIVE_FG = 'Y'
                """
                self.logger.info(f"{query} (params: test_type_key = {test_type_key})")
                cursor.execute(query, test_type_key=test_type_key)
                return cursor.fetchall()

    def get_test_criteria(self, constraint_key):
        self.logger.info(f"get_test_criteria() for constraint key: {constraint_key}")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT TABLE_NM, COLUMN_NM, TEST_CRITERIA_NM 
                FROM {self._schema}.FCEOLQT_TEST_CRITERIA_DIM CRITERIA,
                    (
                    SELECT FCEOLQT_TEST_CRITERIA_DIM_KY
                    FROM {self._schema}.FCEOLQT_WO_TEST_CNSTR_DIM
                    WHERE FCEOLQT_WO_TEST_CNSTR_DIM_KY = :constraint_key
                    ) CNSTR
                WHERE CRITERIA.FCEOLQT_TEST_CRITERIA_DIM_KY = 
                CNSTR.FCEOLQT_TEST_CRITERIA_DIM_KY
                """
                self.logger.info(f"{query} (params: constraint_key = {constraint_key})")
                cursor.execute(query, constraint_key=constraint_key)
                return cursor.fetchone()

    def get_all_work_orders(self, filters):
        self.logger.info("get_all_work_orders()")
        with self.get_pool().acquire() as connection:
            placeholders = ", ".join(f":filter{i}" for i in filters)
            with connection.cursor() as cursor:
                query = f"""SELECT 
                                WOD.WORK_ORDER_ID, WOD.INV_ITEM_DIM_KY 
                            FROM 
                                {self._schema}.WORK_ORDER_DIM WOD, {self._schema}.INV_ITEM_DIM IID 
                            WHERE 
                                WOD.WORK_ORDER_STATUS_NM = 'Closed' AND WOD.WORK_ORDER_DEST_NM = 'FGI' AND
                                WOD.UPDATE_DM > SYSDATE - 270 AND IID.INV_ITEM_DIM_KY = WOD.INV_ITEM_DIM_KY AND 
                                IID.PART_TYPE_NM not in ('DRY PEN','PEN BODY') AND
                                WOD.INV_ITEM_DIM_KY NOT IN ({placeholders}) AND 
                                WOD.WORK_ORDER_ID NOT IN 
                                    (SELECT WORK_ORDER_ID FROM {self._schema}.FCEOLQT_WO_RESULT_FACT)"""

                self.logger.info(f"{query} (params: filters = {filters})")
                cursor.execute(query, filters)
                return cursor.fetchall()

    def get_work_order(self, work_order_id):
        self.logger.info(f"get_work_order() for work order id: {work_order_id}")

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT 
                                WOD.WORK_ORDER_ID, WOD.INV_ITEM_DIM_KY  
                            FROM 
                                {self._schema}.WORK_ORDER_DIM WOD, 
                                {self._schema}.INV_ITEM_DIM IID 
                            WHERE WOD.WORK_ORDER_STATUS_NM = 'Closed' 
                                    AND WOD.WORK_ORDER_DEST_NM = 'FGI' 
                                    AND IID.INV_ITEM_DIM_KY = WOD.INV_ITEM_DIM_KY 
                                    AND IID.PART_TYPE_NM not in ('DRY PEN','PEN BODY')
                                    AND WOD.WORK_ORDER_ID = :work_order_id"""
                self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, work_order_id=work_order_id)
                return cursor.fetchone()

    def get_printed_count_for_falcap(self, work_order_id):
        self.logger.info(f"get_printed_count_for_falcap() for work order id: {work_order_id}")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                universal_query = SharedQueries.get_printed_count_for_falcap(self._schema)
                self.logger.info(f"{universal_query.oracle_query} (params: work_order_id = {work_order_id})")
                return self.execute_universal_query_one(cursor, universal_query, work_order_id=work_order_id)
        # self.logger.info(
        #     f"get_printed_count_for_falcap() for work order id: {work_order_id}"
        # )
        # with self.get_pool().acquire() as connection:
        #     with connection.cursor() as cursor:
        #         query = f"""SELECT COUNT(*) FROM {self._schema}.PEN_PROCESS_FACT PPF, {self._schema}.PEN_INFO_DIM PID
        #                     WHERE PID.LAST_WORK_ORDER_ID = :work_order_id
        #                     AND PID.PN_ID = PPF.PN_ID
        #                     AND PPF.PROCESS_STEP_DIM_KY = 2130"""
        #         self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
        #         cursor.execute(query, work_order_id=work_order_id)
        #         return cursor.fetchone()

    def get_ink_type_dim_ky(self, work_order_id):
        self.logger.info(f"get_ink_type_dim_ky() for work order id: {work_order_id}")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT DISTINCT PSF.INK_TYPE_DIM_KY
                            FROM {self._schema}.PEN_INFO_DIM PID, {self._schema}.PEN_SLOT_FACT PSF
                            WHERE PID.LAST_WORK_ORDER_ID = :work_order_id AND
                            PSF.PN_ID = PID.PN_ID AND
                            PSF.INK_TYPE_DIM_KY IS NOT NULL"""
                self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, work_order_id=work_order_id)
                return cursor.fetchall()

    def get_arch_id(self, work_order_id):
        self.logger.info(f"get_arch_id() for work order id: {work_order_id}")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT DISTINCT DID.ARCH_ID
                            FROM {self._schema}.PEN_INFO_DIM PID, {self._schema}.PEN_SLOT_FACT PSF,
                            {self._schema}.DIE_INFO_DIM DID
                            WHERE PID.LAST_WORK_ORDER_ID = :work_order_id AND
                            PSF.PN_ID = PID.PN_ID AND
                            DID.DIE_INFO_DIM_KY = PSF.DIE_INFO_DIM_KY"""
                self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, work_order_id=work_order_id)
                return cursor.fetchall()

    def get_prod_colors(self, work_order_id):
        self.logger.info(f"get_prod_color() for work order id: {work_order_id}")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT DISTINCT PSF.PROD_COLOR_DIM_KY
                            FROM  {self._schema}.PEN_INFO_DIM PID, {self._schema}.PEN_NOZZLE_COLUMN_FACT PSF
                            WHERE PID.LAST_WORK_ORDER_ID = :work_order_id AND PID.PN_ID = PSF.PN_ID"""
                self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, work_order_id=work_order_id)
                return cursor.fetchall()

    def get_pen_count(self, work_order_id, inv_item_ky, filters=None):
        self.logger.info(
            f"get_pen_count() for work order id: {work_order_id}, inv_item_ky: {inv_item_ky}, filters: {filters}"
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT COUNT(DISTINCT PSF.PN_ID)
                            FROM {self._schema}.PEN_SLOT_FACT PSF, {self._schema}.PEN_INFO_DIM PID
                            WHERE
                                PID.LAST_WORK_ORDER_ID = :work_order_id 
                                AND PID.PN_ID = PSF.PN_ID
                                AND PSF.CAP_CLOU_TEST_DM IS NOT NULL """

                if filters is None or inv_item_ky not in filters:
                    query += f"""AND PSF.HUE2_TEST_DM IS NOT NULL"""

                self.logger.info(f"{query} (params: work_order_id = {work_order_id})")

                cursor.execute(query, work_order_id=work_order_id)
                return cursor.fetchone()

    def get_color_keys(self, prod_color_nm):
        self.logger.info(
            f"""get_color_keys() for prod_color_nm: {prod_color_nm}"""
        )
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                # Handle tuple of color names for IN clause
                if isinstance(prod_color_nm, (tuple, list)):
                    # Create placeholders for IN clause
                    placeholders = ', '.join([':color' + str(i) for i in range(len(prod_color_nm))])
                    # Create parameter dictionary
                    params = {f'color{i}': color for i, color in enumerate(prod_color_nm)}
                else:
                    placeholders = ':color0'
                    params = {'color0': prod_color_nm}
                
                query = f"""
                    SELECT 
                        DISTINCT sel.PROD_COLOR_DIM_KY
                    FROM 
                        {self._schema}.PRODUCT_COLOR_DIM sel
                    WHERE 
                        sel.prod_color_nm IN ({placeholders}) AND prod_color_dim_ky <> 36
                    """

                self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                return cursor.fetchall()
            


    def get_pen_count_from_parametric_data(
        self,
        work_order_id,
        table_name,
        slot_type_cd,
        prod_color_dim_ky,
    ):
        self.logger.info(
            f"""get_pen_count_from_parametric_data() for work order id: {work_order_id}, 
            table_name: {table_name}, slot_type_cd: {slot_type_cd}"""
        )
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                params = [work_order_id, slot_type_cd]
                query = f"""SELECT COUNT(DISTINCT sel.PN_ID)
                            FROM 
                                {self._schema}.{table_name} sel 
                                    join {self._schema}.SLOT_DIM sd on sd.slot_dim_ky = sel.slot_dim_ky,
                                (
                                    SELECT PN_ID 
                                    FROM {self._schema}.PEN_INFO_DIM
                                    WHERE LAST_WORK_ORDER_ID = :work_order_id
                                ) wo
                            WHERE 
                                sel.PN_ID = wo.PN_ID 
                                AND NVL(sd.SLOT_TYPE_CD, 'NULL') = NVL(:slot_type_cd, 'NULL')
                            """

                if prod_color_dim_ky is not None:
                    query += " AND sel.PROD_COLOR_DIM_KY = :prod_color_dim_ky"
                    params.append(prod_color_dim_ky)

                self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                return cursor.fetchone()
        pass

    def get_test_parametric_data(
        self,
        work_order_id,
        table_name,
        column_name,
        slot_type_cd,
        prod_color_dim_ky,
        filters_prod_colors=None,
    ):
        self.logger.info(
            f"""get_test_parametric_data() for work order id: {work_order_id}, table_name: {table_name}, 
            column_name: {column_name}, slot_type_cd: {slot_type_cd}"""
        )
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                params = [work_order_id, slot_type_cd]
                query = f"""SELECT {column_name} 
                            FROM {self._schema}.{table_name} sel 
                                join {self._schema}.SLOT_DIM sd on sd.slot_dim_ky = sel.slot_dim_ky,
                                (SELECT PN_ID 
                                    FROM {self._schema}.PEN_INFO_DIM
                                    WHERE LAST_WORK_ORDER_ID = :work_order_id) wo
                            WHERE sel.PN_ID = wo.PN_ID AND
                                  NVL(sd.SLOT_TYPE_CD, 'NULL') = NVL(:slot_type_cd,'NULL')"""

                # if constraint prod_color_dim_ky is not None, it means that we have to filter by color
                if prod_color_dim_ky is not None:
                    query += " AND sel.PROD_COLOR_DIM_KY = :prod_color_dim_ky"
                    params.append(prod_color_dim_ky)
                else:
                    index = 0
                    if filters_prod_colors is not None:
                        for filter_prod_color_dim_ky in filters_prod_colors:
                            query += f" AND NOT sel.PROD_COLOR_DIM_KY = :prod_color_dim_ky_{index}"
                            params.append(filter_prod_color_dim_ky)
                            index += 1

                query += f" AND {column_name} IS NOT NULL ORDER BY {column_name} DESC"
                self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                return cursor.fetchall()

    """
    This method returns the constraint colors for a given work order test key, criteria_key and slot_type_cd.
    used by get_test_parametric_data() method
    """

    def _get_constraint_colors(self, test_type_key, criteria_key, slot_type_cd):
        self.logger.info(
            f"""_get_constraint_colors() for test_type_key: {test_type_key}, criteria_key: {criteria_key},
             slot_type_cd: {slot_type_cd}"""
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT PROD_COLOR_DIM_KY
                            FROM
                                {self._schema}.FCEOLQT_WO_TEST_CNSTR_DIM
                            WHERE
                                FCEOLQT_TEST_CRITERIA_DIM_KY = :criteria_key AND
                                FCEOLQT_WO_TYPE_DIM_KY = :test_type_key AND 
                                NVL(SLOT_TYPE_CD,'NULL') = NVL(:slot_type_cd,'NULL')"""
                self.logger.info(
                    f"{query} (params: {test_type_key}, {criteria_key}, {slot_type_cd})"
                )
                cursor.execute(query, [test_type_key, criteria_key, slot_type_cd])
                return cursor.fetchall()

    def get_dates_to_failure(self, work_order_id, days_to_process_wo_ct):
        self.logger.info(
            f"get_dates_to_failure() for work order id: {work_order_id}, days_to_process_wo_ct: {days_to_process_wo_ct}"
        )
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT
                        CASE WHEN MAX_DATE.CALENDAR_DT < TRUNC(SYSDATE)
                                    THEN
                                        1
                                    ELSE
                                        0
                        END AS CASE
                        FROM (
                            SELECT CALENDAR_DT FROM (
                                SELECT * FROM (
                                    SELECT D.* FROM
                                        {self._schema}.DATE_DAY_DIM D,
                                        (
                                            SELECT TRUNC(WORK_ORDER_CLOSE_DM) as CLOSE_DM
                                            FROM
                                                {self._schema}.WORK_ORDER_DIM
                                            WHERE
                                                WORK_ORDER_ID = :work_order_id
                                        ) C
                                    WHERE
                                        D.CALENDAR_DT >= C.CLOSE_DM
                                        AND ROWNUM <= :days_to_process_wo_ct
                                        AND D.HP_US_WORKDAY_OF_MONTH_NR IS NOT NULL
                                        AND D.HP_US_HOLIDAY_FG != 'Y'
                                        ORDER BY D.CALENDAR_DT ASC )
                                    ORDER BY CALENDAR_DT DESC )
                        WHERE ROWNUM <=1 ) MAX_DATE"""
                self.logger.info(
                    f"""{query} (params: work_order_id = {work_order_id}, 
                    days_to_process_wo_ct = {days_to_process_wo_ct})"""
                )
                cursor.execute(
                    query,
                    work_order_id=work_order_id,
                    days_to_process_wo_ct=days_to_process_wo_ct,
                )
                return cursor.fetchone()

    def block_work_orders(self, work_orders):
        work_order_ids = [(x.id,) for x in work_orders]
        self.logger.info(f"block_work_orders() for work order ids: {work_order_ids}")

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""INSERT INTO {self._schema}.FCEOLQT_WO_RESULT_FACT (FCEOLQT_WORSLT_KY, WORK_ORDER_ID, 
                            STATUS_CD, EMAIL_SENT_FG, LATEST_RUN_DM)
                            VALUES ({self._schema}.FCEOLQT_WO_RESULT_SEQ.NEXTVAL, :work_order_id, 'B', 'N', SYSDATE)
                        """
                self.logger.info(f"{query} (params: work_order_ids = {work_order_ids})")
                if not self._should_insert:
                    self.logger.info(
                        "Not issuing insert because should_insert is False"
                    )

                if self._should_insert:
                    cursor.executemany(query, work_order_ids)
                    connection.commit()

    def set_email_sent(self, work_orders):
        work_orders_id = [work_order.id for work_order in work_orders]
        self.logger.info(f"set_email_sent() for work order ids: {work_orders_id}")
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                for work_order in work_orders:
                    # get latest fceolqt_worslt_ky
                    query = f"""SELECT FCEOLQT_WORSLT_KY FROM {self._schema}.FCEOLQT_WO_RESULT_FACT 
                                WHERE WORK_ORDER_ID = :work_order_id
                                ORDER BY LATEST_RUN_DM DESC
                                """
                    self.logger.info(
                        f"{query} (params: work_order_id = {work_order.id})"
                    )

                    cursor.execute(query, work_order_id=work_order.id)
                    result = cursor.fetchone()

                    if result:
                        fceolqt_worslt_ky = result[0]
                        query = f"""UPDATE {self._schema}.FCEOLQT_WO_RESULT_FACT SET EMAIL_SENT_FG = 'Y' 
                                    WHERE FCEOLQT_WORSLT_KY = :fceolqt_worslt_ky
                                """
                        self.logger.info(
                            f"{query} (params: fceolqt_worslt_ky = {fceolqt_worslt_ky})"
                        )
                        if not self._should_update:
                            self.logger.info(
                                "Not issuing update because should_update is False"
                            )

                        if self._should_update:
                            cursor.execute(query, fceolqt_worslt_ky=fceolqt_worslt_ky)
                            connection.commit()

  

    def update_wo_test_fact(self, work_order_id, constraint_key, pass_fail_result):
        self.logger.info(
            f"""update_wo_test_fact() for work order id: {work_order_id}, constraint_key: {constraint_key}, 
            result: {pass_fail_result}"""
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT COUNT(FCEOLQT_WO_TEST_CNSTR_DIM_KY)
                            FROM {self._schema}.FCEOLQT_WO_TEST_FACT
                            WHERE
                                WORK_ORDER_ID = :work_order_id AND
                                FCEOLQT_WO_TEST_CNSTR_DIM_KY = :constraint_key"""
                self.logger.info(
                    f"{query} (params: work_order_id = {work_order_id}, constraint_key = {constraint_key})"
                )
                cursor.execute(query, [work_order_id, constraint_key])
                exist = cursor.fetchone()
                self.logger.info(f"exist count: {exist[0]}")
                update_query = None
                if exist[0] == 0:
                    update_query = f"""INSERT INTO {self._schema}.FCEOLQT_WO_TEST_FACT (WORK_ORDER_ID, 
                                FCEOLQT_WO_TEST_CNSTR_DIM_KY, PASS_FAIL_FG, LATEST_RUN_DM)
                                VALUES (:work_order_id, :constraint_key, 
                                :pass_fail_result, SYSDATE)"""
                else:
                    update_query = f"""UPDATE {self._schema}.FCEOLQT_WO_TEST_FACT
                                SET PASS_FAIL_FG = :pass_fail_result, LATEST_RUN_DM=SYSDATE
                                WHERE WORK_ORDER_ID = :work_order_id AND FCEOLQT_WO_TEST_CNSTR_DIM_KY = 
                                :constraint_key"""

                self.logger.info(
                    f"""{update_query} (params: work_order_id = {work_order_id}, constraint_key = {constraint_key},
                     pass_fail_result = {pass_fail_result})"""
                )

                if not self._should_update or not self._should_insert:
                    self.logger.info(
                        "Not issuing update and insert because should_update and should_insert are False"
                    )

                if self._should_update and self._should_insert:
                    cursor.execute(
                        update_query,
                        work_order_id=work_order_id,
                        constraint_key=constraint_key,
                        pass_fail_result=pass_fail_result,
                    )
                    connection.commit()

    def get_delta_e_coordinates_for_slot_type_cd(
        self, work_order_id,  slot_type_cd, prod_color_dim_ky
    ):
        self.logger.info(
            f"""get_delta_e_coordinates_for_slot_type_cd() for work order id: {work_order_id},
                prod_color_dim_ky key: {prod_color_dim_ky}, slot type cd: {slot_type_cd}"""
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT /*+ LEADING */  median (psf.hue2_astar_av), median (psf.hue2_bstar_av), median (psf.hue2_lstar_av)
                            FROM
                                {self._schema}.PEN_SLOT_FACT PSF
                                JOIN {self._schema}.SLOT_DIM SD ON SD.SLOT_DIM_KY = PSF.SLOT_DIM_KY,
                                {self._schema}.PEN_INFO_DIM PID,
                                (
                                    SELECT
                                        /*+ LEADING */ *
                                    FROM
                                        (
                                            SELECT
                                                /*+ LEADING */
                                                DISTINCT WORK_ORDER_ID,
                                                WORK_ORDER_CLOSE_DM
                                            FROM
                                                (
                                                    SELECT
                                                        /*+ LEADING */
                                                        DISTINCT PID2.*,
                                                        WOD.*
                                                    FROM
                                                        {self._schema}.PEN_INFO_DIM PID2,
                                                        {self._schema}.WORK_ORDER_DIM WOD,
                                                        {self._schema}.PRODUCT_FAMILY_DIM PFD,
                                                        {self._schema}.PEN_SLOT_FACT PSF2
                                                        JOIN {self._schema}.SLOT_DIM SD2 ON SD2.SLOT_DIM_KY = PSF2.SLOT_DIM_KY,
                                                        (
                                                            SELECT
                                                                /*+ LEADING */
                                                                (
                                                                    SELECT
                                                                        /*+ LEADING */
                                                                        DISTINCT pfd2.prod_group_dim_ky
                                                                    FROM
                                                                        {self._schema}.product_family_dim pfd2,
                                                                        {self._schema}.work_order_dim wod2
                                                                    WHERE
                                                                        wod2.prod_family_dim_ky = pfd2.prod_family_dim_ky
                                                                        AND wod2.work_order_id = :work_order_id
                                                                ) as prod_group_dim_ky,
                                                                (
                                                                    SELECT
                                                                        /*+ LEADING */
                                                                        DISTINCT pid3.odd_ink_type_nm
                                                                    FROM
                                                                        {self._schema}.pen_info_dim pid3
                                                                    WHERE
                                                                        pid3.last_affect_yld_work_order_id = :work_order_id
                                                                        AND pid3.odd_ink_type_nm IS NOT NULL
                                                                ) as odd_ink_type_nm,
                                                                (
                                                                    SELECT
                                                                        /*+ LEADING */
                                                                        DISTINCT pid3.odd_ink_color_nm
                                                                    FROM
                                                                        {self._schema}.pen_info_dim pid3
                                                                    WHERE
                                                                        pid3.last_affect_yld_work_order_id = :work_order_id
                                                                        AND pid3.odd_ink_type_nm IS NOT NULL
                                                                ) as odd_ink_color_nm,
                                                                (
                                                                    SELECT
                                                                        /*+ LEADING */
                                                                        DISTINCT pid3.even_ink_color_nm
                                                                    FROM
                                                                        {self._schema}.pen_info_dim pid3
                                                                    WHERE
                                                                        pid3.last_affect_yld_work_order_id = :work_order_id
                                                                        AND pid3.even_ink_type_nm IS NOT NULL
                                                                ) as even_ink_color_nm,
                                                                (  SELECT  /*+ LEADING */   DISTINCT pid4.arch_id
                                                                    FROM
                                                                        RPTDS.pen_info_dim pid4
                                                                    WHERE
                                                                        pid4.last_affect_yld_work_order_id =  :work_order_id
                                                                        AND pid4.odd_ink_type_nm IS NOT NULL
                                                                ) as arch_id
                                                            from
                                                                DUAL
                                                        ) WO
                                                    WHERE
                                                        PID2.LAST_WORK_ORDER_ID != :work_order_id
                                                        AND PID2.EVEN_INK_COLOR_NM = WO.EVEN_INK_COLOR_NM
                                                        AND PID2.odd_ink_color_nm = WO.odd_ink_color_nm
                                                        AND PID2.odd_ink_type_nm = WO.odd_ink_type_nm
                                                        AND PID2.arch_id = WO.arch_id
                                                        AND PID2.last_affect_yld_work_order_id = WOD.WORK_ORDER_ID
                                                        AND wod.prod_family_dim_ky = pfd.prod_family_dim_ky
                                                        AND PFD.prod_group_dim_ky = WO.prod_group_dim_ky
                                                        AND PID2.last_affect_yld_work_order_id != :work_order_id
                                                        AND WOD.WORK_ORDER_STATUS_NM = 'Closed'
                                                        AND WOD.WORK_ORDER_DEST_NM = 'FGI'
                                                        AND WOD.WORK_ORDER_CLOSE_DM < (SYSDATE - 15) AND WORK_ORDER_CLOSE_DM > (SYSDATE - 200)
                                                        AND PID2.PN_ID = PSF2.PN_ID
                                                        AND PSF2.CAP_CLOU_TEST_DM IS NOT NULL
                                                        AND PSF2.HUE2_TEST_DM IS NOT NULL
                                                        AND NVL(SD2.SLOT_TYPE_CD, 'NULL') = NVL(:slot_type_cd, 'NULL')
                                                )
                                            ORDER BY
                                                WORK_ORDER_CLOSE_DM DESC
                                        )
                                    WHERE
                                        ROWNUM <= 30
                                ) WORK_ORDERS
                            WHERE
                                PSF.PN_ID = PID.PN_ID
                                AND PID.LAST_WORK_ORDER_ID = WORK_ORDERS.WORK_ORDER_ID
                                AND PSF.CAP_CLOU_TEST_DM IS NOT NULL
                                AND PSF.HUE2_TEST_DM IS NOT NULL
                                AND PSF.PROD_COLOR_DIM_KY = :prod_color_dim_ky
                                AND NVL(SD.SLOT_TYPE_CD, 'NULL') = NVL(:slot_type_cd, 'NULL')
                            """
                self.logger.info(
                    f"""{query} (params: work_order_id = {work_order_id}, prod_color_dim_ky = {prod_color_dim_ky}, 
                        slot_type_cd = {slot_type_cd})"""
                )
                cursor.execute(
                    query,
                    work_order_id=work_order_id,
                    slot_type_cd=slot_type_cd,
                    prod_color_dim_ky=prod_color_dim_ky,
                )
                return cursor.fetchall()

    def get_pens_per_slot_type_cd(self, work_order_id, slot_type_cd, prod_color_dim_ky=None):
        self.logger.info(
            f"get_pens_per_slot_type_cd() for work order id: {work_order_id}, slot type cd: {slot_type_cd}, prod color dim ky: {prod_color_dim_ky}"
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f""" SELECT PSF.*
                                FROM 
                                    {self._schema}.PEN_SLOT_FACT PSF 
                                    join {self._schema}.SLOT_DIM SD on SD.SLOT_DIM_KY = PSF.SLOT_DIM_KY,
                                    {self._schema}.PEN_INFO_DIM PID
                                WHERE
                                    PID.LAST_WORK_ORDER_ID = :work_order_id AND
                                    PSF.PN_ID = PID.PN_ID AND PSF.PROD_COLOR_DIM_KY = :prod_color_dim_ky AND
                                    PSF.HUE2_TEST_DM IS NOT NULL AND
                                    NVL(SD.SLOT_TYPE_CD,'NULL') = NVL(:slot_type_cd,'NULL')"""
                self.logger.info(
                    f"{query} (params: work_order_id = {work_order_id}, slot_type_cd = {slot_type_cd}, prod_color_dim_ky = {prod_color_dim_ky})"
                )
                cursor.execute(
                    query, work_order_id=work_order_id, slot_type_cd=slot_type_cd, prod_color_dim_ky=prod_color_dim_ky
                )

                return cursor.fetchall()

    def get_test_types(self, arch_id, ink_type_dim_ky):
        self.logger.info(
            f"get_test_types() for arch id: {arch_id} and ink type dim ky: {ink_type_dim_ky}"
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT FCEOLQT_WO_TYPE_DIM_KY, WORK_ORDER_TYPE_NM, INK_TYPE_DIM_KY, ARCH_ID, MIN_PEN_CT,
                                   DAYS_TO_PROCESS_WO_CT, ACTIVE_FG
                            FROM {self._schema}.FCEOLQT_WO_TYPE_DIM
                            WHERE ARCH_ID = :arch_id AND INK_TYPE_DIM_KY = :ink_type_dim_ky AND ACTIVE_FG = 'Y'"""
                self.logger.info(
                    f"{query} (params: arch_id = {arch_id}, ink_type_dim_ky = {ink_type_dim_ky})"
                )
                cursor.execute(query, arch_id=arch_id, ink_type_dim_ky=ink_type_dim_ky)
                return cursor.fetchall()

    def update_delta_e(self, pn_id, die_site_nr, slot_dim_ky, delta_e):
        # self.logger.info(
        #     f"""update_delta_e() for pn_id: {pn_id}, die_site_nr: {die_site_nr}, slot_dim_ky: {slot_dim_ky},
        #      delta_e: {delta_e}"""
        # )
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""UPDATE {self._schema}.PEN_SLOT_FACT
                            SET DELTA_E_VL_2 = :delta_e
                            WHERE 
                                PN_ID = :pn_id AND 
                                DIE_SITE_NR = :die_site_nr AND
                                SLOT_DIM_KY = :slot_dim_ky"""
                # self.logger.info(
                #     f"""{query} (params: pn_id = {pn_id}, die_site_nr = {die_site_nr}, slot_dim_ky = {slot_dim_ky},
                #      delta_e = {delta_e})"""
                # )
                # if not self._should_update:
                #     self.logger.info(
                #         "Not issuing update because should_update is False"
                #     )
                
              
                if self._should_update:
                    cursor.execute(
                        query,
                        pn_id=pn_id,
                        die_site_nr=die_site_nr,
                        slot_dim_ky=slot_dim_ky,
                        delta_e=delta_e,
                    )
                    connection.commit()

    def calc_percentile_using_stored_function(self, data, pct):
        self.logger.info(
            f"calc_percentile_using_stored_function() for pct: {pct}, data length: {len(data)}"
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                result_list_type = connection.gettype(
                    f"{self._schema}.FCEOLQT.RESULT_LIST"
                )
                self.logger.info(
                    f"Getting type from oracle for {self._schema}.FCEOLQT.RESULT_LIST"
                )
                array = result_list_type.newobject()
                array.extend(data)
                result = cursor.callfunc(
                    f"{self._schema}.FCEOLQT.CALC_PERCENTILE",
                    oracledb.NUMBER,
                    [array, pct],
                )
                self.logger.info(f"Result from oracle: {result}")
                return result

    def store_wo_result_falconDB(self, work_order_id, result):
        self.logger.info(
            f"store_wo_result_falconDB() for work order id: {work_order_id}, result: {result}"
        )

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""INSERT INTO {self._schema}.FCEOLQT_WO_RESULT_FACT (FCEOLQT_WORSLT_KY, WORK_ORDER_ID, 
                            STATUS_CD, EMAIL_SENT_FG, LATEST_RUN_DM)
                            VALUES ({self._schema}.FCEOLQT_WO_RESULT_SEQ.NEXTVAL, :work_order_id, :result, 'N', SYSDATE)
                        """
                self.logger.info(
                    f"{query} (params: work_order_id = {work_order_id}, result = {result})"
                )
                if not self._should_insert:
                    self.logger.info(
                        "Not issuing insert because should_insert is False"
                    )

                if self._should_insert:
                    cursor.execute(query, work_order_id=work_order_id, result=result)
                    connection.commit()

    def get_failure_reasons(self, work_order_id):
        self.logger.info(f"get_failure_reasons() for work order id: {work_order_id}")

        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                query = f"""SELECT TCRI.TEST_CRITERIA_NM, PC.PROD_COLOR_NM
                            FROM {self._schema}.FCEOLQT_WO_TEST_FACT TF 
                                inner join {self._schema}.FCEOLQT_WO_TEST_CNSTR_DIM TCNSTR
                                    on TCNSTR.FCEOLQT_WO_TEST_CNSTR_DIM_KY = TF.FCEOLQT_WO_TEST_CNSTR_DIM_KY
                                inner join {self._schema}.FCEOLQT_TEST_CRITERIA_DIM TCRI
                                    on TCNSTR.FCEOLQT_TEST_CRITERIA_DIM_KY = TCRI.FCEOLQT_TEST_CRITERIA_DIM_KY
                                left outer join {self._schema}.PRODUCT_COLOR_DIM PC 
                                    on TCNSTR.PROD_COLOR_DIM_KY = PC.PROD_COLOR_DIM_KY
                            WHERE
                                TF.WORK_ORDER_ID = :work_order_id AND
                                TF.PASS_FAIL_FG = 'F'
                            ORDER BY TCNSTR.FCEOLQT_WO_TEST_CNSTR_DIM_KY
                        """
                self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, work_order_id=work_order_id)
                return cursor.fetchall()

    def get_work_orders_with_no_email_sent(self, tested_work_order_ids):
        self.logger.info(f"get_work_orders_with_no_email_sent()")
        results = []
        with self.get_pool().acquire() as connection:
            with connection.cursor() as cursor:
                tested_work_order_ids_query = ""
                if tested_work_order_ids and len(tested_work_order_ids) != 0:
                    tested_work_order_ids_query = "AND NOT WORK_ORDER_ID IN ("
                    for index, tested_work_order_id in enumerate(tested_work_order_ids):
                        tested_work_order_ids_query += f":tested_work_order_id_{index}"
                        if index < len(tested_work_order_ids) - 1:
                            tested_work_order_ids_query += ","
                    tested_work_order_ids_query += ")"

                query = f"""SELECT DISTINCT WORK_ORDER_ID
                            FROM (
                                SELECT * FROM {self._schema}.FCEOLQT_WO_RESULT_FACT
                                WHERE EMAIL_SENT_FG = 'N'
                                AND NOT WORK_ORDER_ID IN (
                                    SELECT WORK_ORDER_ID 
                                    FROM {self._schema}.FCEOLQT_WO_RESULT_FACT
                                    WHERE EMAIL_SENT_FG = 'Y'
                                ) {tested_work_order_ids_query}
                            )
                        """
                self.logger.info(
                    f"{query} (params: tested_work_order_ids = {tested_work_order_ids})"
                )
                cursor.execute(query, tested_work_order_ids)
                for row in cursor.fetchall():
                    query = f"""SELECT STATUS_CD 
                                FROM (
                                    SELECT * FROM {self._schema}.FCEOLQT_WO_RESULT_FACT WOR
                                    WHERE WOR.WORK_ORDER_ID = :work_order_id
                                    ORDER BY WOR.LATEST_RUN_DM DESC
                                )
                                WHERE ROWNUM = 1
                            """
                    self.logger.info(f"{query} (params: work_order_id = {row[0]})")
                    cursor.execute(query, work_order_id=row[0])
                    result = cursor.fetchone()
                    if result:
                        results.append((row[0], result[0]))
        return results

    def get_pool(self):
        return self._pool
