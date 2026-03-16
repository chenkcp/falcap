import logging
import sys
import psycopg2
from psycopg2 import pool
import os

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

class PostgresProvider(DBProvider):
    logger = logging.getLogger(__name__)
    _pool = None
     
    def __init__(
        self,
        user,
        password,
        host,
        port,
        dbname,
        schema,
        should_update=False,
        should_insert=False,
    ):
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._dbname = dbname
        self._schema = schema
        self._should_update = should_update
        self._should_insert = should_insert
        self._pool = None

    def execute_universal_query(self, cursor, universal_query, **params):
        """Execute a universal query with PostgreSQL parameter format."""
        from .QueryAdapter import UniversalQuery
        if isinstance(universal_query, UniversalQuery):
            cursor.execute(universal_query.postgres_query, params)
        else:
            # Handle string queries by converting them
            postgres_query = universal_query.replace(':work_order_id', '%(work_order_id)s')
            postgres_query = postgres_query.replace(':test_type_key', '%(test_type_key)s') 
            postgres_query = postgres_query.replace(':constraint_key', '%(constraint_key)s')
            cursor.execute(postgres_query, params)
        return cursor.fetchall()

    def execute_universal_query_one(self, cursor, universal_query, **params):
        """Execute a universal query and return single row with PostgreSQL parameter format."""
        from .QueryAdapter import UniversalQuery
        if isinstance(universal_query, UniversalQuery):
            cursor.execute(universal_query.postgres_query, params)
        else:
            # Handle string queries by converting them
            postgres_query = universal_query.replace(':work_order_id', '%(work_order_id)s')
            postgres_query = postgres_query.replace(':test_type_key', '%(test_type_key)s') 
            postgres_query = postgres_query.replace(':constraint_key', '%(constraint_key)s')
            cursor.execute(postgres_query, params)
        return cursor.fetchone()

    def init_connection(self):
        self.logger.info("init_connection()")
        
        # Log connection parameters for debugging (excluding password)
        self.logger.info(f"Connecting with: host={self._host}, port={self._port}, database={self._dbname}, user={self._user}")
        
        # Validate required parameters
        if not all([self._host, self._port, self._dbname, self._user, self._password]):
            missing = [param for param, value in [
                ('host', self._host), ('port', self._port), 
                ('database', self._dbname), ('user', self._user), 
                ('password', self._password)
            ] if not value]
            raise ValueError(f"Missing required connection parameters: {missing}")
        
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                host=self._host,
                port=self._port,
                database=self._dbname,
                user=self._user,
                password=self._password
            )
        except Exception as e:
            self.logger.error(f"Failed to create connection pool: {e}")
            raise
        
        try:
            # Test connection
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()
                    self.logger.info(f"Connected to PostgreSQL: {version[0]}")
            finally:
                self._pool.putconn(conn)
        except Exception as e:
            self.logger.error(f"Error during database access: {e}")
            self.logger.error(f"Connection details: host={self._host}:{self._port}, database={self._dbname}, user={self._user}")
            raise

    def get_pool(self):
        return self._pool

    def is_connected(self):
        self.logger.info("is_connected()")
        try:
            conn = self._pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
            finally:
                self._pool.putconn(conn)
        except Exception as e:
            self.logger.error(f"PostgresProvider.is_connected() exception: {e}")
            return False

    def close_connection(self):
        self.logger.info("close_connection()")
        if self._pool:
            self._pool.closeall()
            
    def set_database_operations(self, should_update, should_insert):
        self._should_update = should_update
        self._should_insert = should_insert

    def _convert_decimal_columns(self, rows, columns=None, convert_all_ky=True, additional_columns=None):
        """
        Generic method to convert Decimal values to integers and datetime objects to strings in database results.
        
        Args:
            rows: Raw database rows (list of tuples or list of row objects)
            columns: Column names (optional, for dictionary conversion)
            convert_all_ky: If True, automatically converts all columns ending with '_ky'
            additional_columns: List of additional column names to convert
        
        Returns:
            Processed rows with Decimal values converted to integers and datetime objects to ISO strings
        """
        from decimal import Decimal
        from datetime import datetime, date
        
        if not rows:
            return rows
            
        # If columns are provided, work with dictionaries
        if columns:
            result = []
            for row in rows:
                row_dict = dict(zip(columns, row)) if isinstance(row, (tuple, list)) else row
                
                # Convert columns ending with '_ky'
                if convert_all_ky:
                    for key in row_dict:
                        if key.endswith('_ky') and row_dict[key] is not None:
                            if isinstance(row_dict[key], Decimal):
                                row_dict[key] = int(row_dict[key])
                
                # Convert additional specified columns
                if additional_columns:
                    for key in additional_columns:
                        if key in row_dict and row_dict[key] is not None:
                            if isinstance(row_dict[key], Decimal):
                                row_dict[key] = int(row_dict[key])
                
                # Convert datetime objects to ISO strings in all cases
                for key in row_dict:
                    if row_dict[key] is not None:
                        if isinstance(row_dict[key], datetime):
                            row_dict[key] = row_dict[key].isoformat()
                        elif isinstance(row_dict[key], date):
                            row_dict[key] = row_dict[key].isoformat()
                
                result.append(row_dict)
            return result
        else:
            # Work with tuples/lists - convert all Decimal and datetime values
            result = []
            for row in rows:
                converted_row = []
                for value in row:
                    if isinstance(value, Decimal):
                        converted_row.append(int(value))
                    elif isinstance(value, datetime):
                        converted_row.append(value.isoformat())
                    elif isinstance(value, date):
                        converted_row.append(value.isoformat())
                    else:
                        converted_row.append(value)
                result.append(tuple(converted_row))
            return result

    def get_connection(self):
        """Context manager for database connections"""
        from contextlib import contextmanager
        
        @contextmanager
        def connection_context():
            pool = self.get_pool()
            connection = pool.getconn()
            try:
                yield connection
            finally:
                pool.putconn(connection)
        
        return connection_context()

    def get_all_test_types(self):
        self.logger.info("get_all_test_types()")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT FCEOLQT_WO_TYPE_DIM_KY, WORK_ORDER_TYPE_NM, INK_TYPE_DIM_KY, ARCH_ID, 
                        MIN_PEN_CT, DAYS_TO_PROCESS_WO_CT, ACTIVE_FG 
                    FROM {self._schema}.FCEOLQT_WO_TYPE_DIM
                    WHERE ACTIVE_FG = 'Y'
                """
                #self.logger.info(query)
                cursor.execute(query)
                rows = cursor.fetchall()
                # Use generic conversion method for tuples
                return self._convert_decimal_columns(rows)
            
    def get_constraints(self, test_type_key):
        #self.logger.info(f"get_constraints() for test type key: {test_type_key}")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT FCEOLQT_WO_TEST_CNSTR_DIM_KY, FCEOLQT_TEST_CRITERIA_DIM_KY, PROD_COLOR_DIM_KY, 
                        CONSTRAINT_UPPER_BOUND_VL, CONSTRAINT_LOWER_BOUND_VL, CONSTRAINT_CENTILE_PCT, 
                        SLOT_TYPE_CD, ACTIVE_FG 
                    FROM {self._schema}.FCEOLQT_WO_TEST_CNSTR_DIM
                    WHERE FCEOLQT_WO_TYPE_DIM_KY = %(test_type_key)s AND ACTIVE_FG = 'Y'
                """
                cursor.execute(query, {"test_type_key": test_type_key})
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Convert only columns ending with '_ky' to integers, others to float
                result = []
                for row in rows:
                    converted_row = list(row)
                    from decimal import Decimal
                    
                    for i, (column_name, value) in enumerate(zip(columns, row)):
                        if isinstance(value, Decimal):
                            if column_name.endswith('_ky'):
                                converted_row[i] = int(value)
                            else:
                                converted_row[i] = float(value)
                    
                    result.append(tuple(converted_row))
                return result

    def get_test_criteria(self, constraint_key):
        #self.logger.info(f"get_test_criteria() for constraint key: {constraint_key}")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT TABLE_NM, COLUMN_NM, TEST_CRITERIA_NM 
                    FROM {self._schema}.FCEOLQT_TEST_CRITERIA_DIM CRITERIA
                    JOIN (
                        SELECT FCEOLQT_TEST_CRITERIA_DIM_KY
                        FROM {self._schema}.FCEOLQT_WO_TEST_CNSTR_DIM
                        WHERE FCEOLQT_WO_TEST_CNSTR_DIM_KY = %(constraint_key)s
                    ) CNSTR
                    ON CRITERIA.FCEOLQT_TEST_CRITERIA_DIM_KY = CNSTR.FCEOLQT_TEST_CRITERIA_DIM_KY
                """
                cursor.execute(query, {"constraint_key": constraint_key})
                return cursor.fetchone()

    def get_all_work_orders(self, filters, work_order_id=None):
        if work_order_id is not None:
            self.logger.info(f"get_all_work_orders({work_order_id})")
        else:
            self.logger.debug("get_all_work_orders()")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                placeholders = []
                params = {}
                for i, val in enumerate(filters):
                    key = f"filter{i}"
                    placeholders.append(f"%({key})s")
                    params[key] = val

                #AND WOD.UPDATE_DM > CURRENT_DATE - INTERVAL '5 days'
                query = f"""
                    SELECT WOD.WORK_ORDER_ID, WOD.INV_ITEM_DIM_KY, DATE_TRUNC('day', WORK_ORDER_CLOSE_DM) AS CLOSE_DM
                    FROM {self._schema}.WORK_ORDER_DIM WOD
                    JOIN {self._schema}.INV_ITEM_DIM IID ON IID.INV_ITEM_DIM_KY = WOD.INV_ITEM_DIM_KY
                    WHERE WOD.WORK_ORDER_STATUS_NM = 'Closed'
                    AND WOD.WORK_ORDER_DEST_NM = 'FGI'
                    AND IID.PART_TYPE_NM NOT IN ('DRY PEN','PEN BODY')
                    """
                
                # Only add the NOT IN clause if there are filters to exclude
                if placeholders:
                    query += f"AND WOD.INV_ITEM_DIM_KY NOT IN ({', '.join(placeholders)})\n"
                if work_order_id != None:
                    query += f"""
                    AND WOD.WORK_ORDER_ID = %(work_order_id)s
                    """
                    params["work_order_id"] = work_order_id
                else: 
                    query += f"""
                    AND WOD.UPDATE_DM > CURRENT_DATE - 5 
                    AND WOD.WORK_ORDER_ID NOT IN (
                        SELECT WORK_ORDER_ID FROM {self._schema}.FCEOLQT_WO_RESULT_FACT
                    )
                    """

                self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                rows = cursor.fetchall()
                # Use generic conversion method for tuples
                return self._convert_decimal_columns(rows)


    def get_work_order(self, work_order_id):
        self.logger.info(f"get_work_order() for work order id: {work_order_id}")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT WOD.WORK_ORDER_ID, WOD.INV_ITEM_DIM_KY
                    FROM {self._schema}.WORK_ORDER_DIM WOD
                    JOIN {self._schema}.INV_ITEM_DIM IID ON IID.INV_ITEM_DIM_KY = WOD.INV_ITEM_DIM_KY
                    WHERE WOD.WORK_ORDER_STATUS_NM = 'Closed'
                    AND WOD.WORK_ORDER_DEST_NM = 'FGI'
                    AND WOD.UPDATE_DM > CURRENT_DATE - 5
                    AND IID.PART_TYPE_NM NOT IN ('DRY PEN','PEN BODY')
                    AND WOD.WORK_ORDER_ID = %(work_order_id)s
                """
                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                return cursor.fetchone()

    def get_clou_hue_delta_e_noz_clou_count(self, work_order_id):
        self.logger.info(f"get_clou_hue_delta_e_noz_clou_count() for work order id: {work_order_id}")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    WITH clou AS (
                        SELECT DISTINCT
                            psf.pn_id
                        FROM rptds.pen_slot_fact psf
                        WHERE psf.work_order_id =  %(work_order_id)s
                        AND psf.cap_clou_test_dm IS NOT NULL
                    ),
                    hue AS (
                        SELECT DISTINCT
                            psf.pn_id
                        FROM rptds.pen_slot_fact psf
                        WHERE psf.work_order_id =  %(work_order_id)s
                        AND psf.hue2_test_dm IS NOT NULL
                    ),
                    delta_e_vl_2 AS (
                        SELECT DISTINCT
                                psf.pn_id
                            FROM rptds.pen_slot_fact psf
                            WHERE psf.work_order_id =  %(work_order_id)s
                            AND psf.delta_e_vl_2 IS NOT NULL
                    ),
                    noz_clou AS (
                        SELECT DISTINCT
                            pncf.pn_id
                        FROM rptds.pen_nozzle_column_fact pncf
                        JOIN clou
                        ON clou.pn_id = pncf.pn_id
                        WHERE pncf.cap_clou_test_dm IS NOT NULL
                    )
                    SELECT
                        (SELECT COUNT(*) FROM clou) AS clou_pn_count,
                        (SELECT COUNT(*) FROM hue)  AS hue_pn_count,
                        (SELECT COUNT(*) FROM delta_e_vl_2)  AS delta_e_vl_2_pn_count,
                        (SELECT COUNT(*) FROM noz_clou) AS noz_clou_pn_count;
                """
                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                return cursor.fetchall()
            
    def get_arch_id_color_ink_slot_type_ky(self, work_order_id):
        #self.logger.info(f"get_arch_id_color_ink_slot_type_ky() for work order id: {work_order_id}")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                        SELECT DISTINCT
                            psf.arch_id, psf.prod_color_dim_ky , psf.ink_type_dim_ky , sd.slot_type_cd
                        FROM rptds.pen_slot_fact psf
                        JOIN rptds.slot_dim sd on sd.slot_dim_ky =psf.slot_dim_ky 
                        WHERE psf.work_order_id = %(work_order_id)s
                       
                """
                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                return cursor.fetchall()
        
    
    def get_printed_count_for_falcap(self, work_order_id):
        self.logger.info(f"get_printed_count_for_falcap() for work order id: {work_order_id}")
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                
                universal_query = SharedQueries.get_printed_count_for_falcap(self._schema)
                #self.logger.info(f"{universal_query.postgres_query} (params: work_order_id = {work_order_id})")
                return self.execute_universal_query_one(cursor, universal_query, work_order_id=work_order_id)
            
    def get_ink_type_dim_ky(self, work_order_id):
        self.logger.info(f"get_ink_type_dim_ky() for work order id: {work_order_id}")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT DISTINCT PSF.INK_TYPE_DIM_KY
                    FROM {self._schema}.PEN_SLOT_FACT PSF 
                    WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                    AND PSF.INK_TYPE_DIM_KY IS NOT NULL
                """
                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                return cursor.fetchall()

    
    def get_archId_qry(self, pen_ids):
        self.logger.info(f"get_archId_qry() for pen IDs: {pen_ids}")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Handle empty list
                if not pen_ids:
                    self.logger.info("No pen IDs provided, returning empty result")
                    return []
                
                # Create placeholders for IN clause
                placeholders = ', '.join(['%s' for _ in range(len(pen_ids))])
                
                query = f"""
                    SELECT DISTINCT ARCH_ID 
                    FROM {self._schema}.PEN_SLOT_FACT 
                    WHERE pn_id IN ({placeholders})
                """
                
                self.logger.info(f"{query} (params: pen_ids = {pen_ids})")
                cursor.execute(query, pen_ids)
                return cursor.fetchall()

            
    def get_arch_id(self, work_order_id):
        self.logger.info(f"get_arch_id() for work order id: {work_order_id}")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT DISTINCT DID.ARCH_ID
                    FROM {self._schema}.PEN_SLOT_FACT PSF 
                    JOIN {self._schema}.DIE_INFO_DIM DID ON DID.DIE_INFO_DIM_KY = PSF.DIE_INFO_DIM_KY
                    WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                """
                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                return cursor.fetchall()


    def get_prod_colors(self, work_order_id, slot_type_cd, pen_ids=None):
        self.logger.info(f"get_prod_colors() for work order id: {work_order_id}, slot_type_cd: {slot_type_cd}")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Initialize params dictionary
                params = {
                    "work_order_id": work_order_id,
                    "slot_type_cd": slot_type_cd
                }
                
                # Handle empty pen_ids list
                if not pen_ids:
                    # If no pen_ids provided, get all prod_colors for the work_order and slot_type_cd
                    query = f"""
                        SELECT DISTINCT PSF.PROD_COLOR_DIM_KY
                        FROM {self._schema}.PEN_SLOT_FACT PSF
                        JOIN {self._schema}.SLOT_DIM SD ON PSF.SLOT_DIM_KY = SD.SLOT_DIM_KY
                        WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                        AND COALESCE(SD.SLOT_TYPE_CD, 'NULL') = COALESCE(%(slot_type_cd)s, 'NULL')
                    """
                else:
                    # Create named placeholders for IN clause and add pen_ids to params
                    pen_id_placeholders = []
                    for i, pen_id in enumerate(pen_ids):
                        placeholder_key = f"pen_id_{i}"
                        pen_id_placeholders.append(f"%({placeholder_key})s")
                        params[placeholder_key] = pen_id
                    
                    placeholders = ', '.join(pen_id_placeholders)
                    
                    query = f"""
                        SELECT DISTINCT PSF.PROD_COLOR_DIM_KY
                        FROM {self._schema}.PEN_SLOT_FACT PSF
                        JOIN {self._schema}.SLOT_DIM SD ON PSF.SLOT_DIM_KY = SD.SLOT_DIM_KY
                        WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                        AND COALESCE(SD.SLOT_TYPE_CD, 'NULL') = COALESCE(%(slot_type_cd)s, 'NULL')
                        AND PSF.PN_ID IN ({placeholders})
                    """
                
                #self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                return cursor.fetchall()

    def get_noz_clou_count(self, work_order_id):
        self.logger.info(
            f"get_noz_clou_count() for work order id: {work_order_id}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT COUNT(*)
                    FROM RPTDS.PEN_NOZZLE_COLUMN_FACT
                    WHERE PN_ID IN (
                        SELECT DISTINCT pn_id
                        FROM RPTDS.pen_slot_fact
                        WHERE WORK_ORDER_ID = %(work_order_id)s
                    )
                    AND CAP_CLOU_PAD_DEG_SD IS NOT NULL
                    AND CAP_CLOU_SAD_DEG_SD IS NOT NULL
                    ;
                """

                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                row = cursor.fetchone()
                count = row[0] if row else 0
                return count        
            
    def get_clou_count(self, work_order_id):
        self.logger.info(
            f"get_clou_count() for work order id: {work_order_id}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT COUNT(DISTINCT PSF.PN_ID)
                    FROM {self._schema}.PEN_SLOT_FACT PSF
                    WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                    AND PSF.CAP_CLOU_TEST_DM IS NOT NULL
                """

                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                row = cursor.fetchone()
                count = row[0] if row else 0
                return count   
            
    def get_hue_count(self, work_order_id):
        self.logger.info(
            f"get_hue_count() for work order id: {work_order_id}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT COUNT(DISTINCT PSF.PN_ID)
                    FROM {self._schema}.PEN_SLOT_FACT PSF
                    WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                    AND PSF.HUE2_TEST_DM IS NOT NULL
                """

                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                row = cursor.fetchone()
                count = row[0] if row else 0
                return count   
                    
    def get_delta_e_count(self, work_order_id):
        self.logger.info(
            f"get_delta_e_count() for work order id: {work_order_id}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT COUNT(DISTINCT PSF.PN_ID)
                    FROM {self._schema}.PEN_SLOT_FACT PSF
                    WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                    AND PSF.HUE2_TEST_DM IS NOT NULL AND PSF.DELTA_E_VL_2 IS NOT NULL
                """

                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                row = cursor.fetchone()
                count = row[0] if row else 0
                return count   
            
    def get_pen_count(self, work_order_id, inv_item_ky, filters=None):
        self.logger.info(
            f"get_pen_count() for work order id: {work_order_id}, inv_item_ky: {inv_item_ky}, filters: {filters}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT COUNT(DISTINCT PSF.PN_ID)
                    FROM {self._schema}.PEN_SLOT_FACT PSF
                    WHERE PSF.WORK_ORDER_ID = %(work_order_id)s
                    AND PSF.CAP_CLOU_TEST_DM IS NOT NULL
                """

                if filters is None or inv_item_ky not in filters:
                    query += " AND PSF.HUE2_TEST_DM IS NOT NULL"

                #self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                return cursor.fetchone()

       
    def get_pen_count_from_parametric_data(self, work_order_id, table_name, slot_type_cd, prod_color_dim_ky):
        self.logger.info(
            f"get_pen_count_from_parametric_data() for work order id: {work_order_id}, "
            f"table_name: {table_name}, slot_type_cd: {slot_type_cd}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                params = {
                    "work_order_id": work_order_id,
                    "slot_type_cd": slot_type_cd
                }

                query = f"""
                    SELECT COUNT(DISTINCT sel.PN_ID)
                    FROM {self._schema}.{table_name} sel
                    JOIN {self._schema}.SLOT_DIM sd ON sd.slot_dim_ky = sel.slot_dim_ky,
                        (SELECT PN_ID FROM {self._schema}.PEN_SLOT_FACT WHERE WORK_ORDER_ID = %(work_order_id)s) wo
                    WHERE sel.PN_ID = wo.PN_ID
                    AND COALESCE(sd.SLOT_TYPE_CD, 'NULL') = COALESCE(%(slot_type_cd)s, 'NULL')
                """

                if prod_color_dim_ky is not None:
                    query += " AND sel.PROD_COLOR_DIM_KY = %(prod_color_dim_ky)s"
                    params["prod_color_dim_ky"] = prod_color_dim_ky

                #self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                return cursor.fetchone()

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
            f"get_test_parametric_data() for work order id: {work_order_id}, table_name: {table_name}, "
            f"column_name: {column_name}, slot_type_cd: {slot_type_cd}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                params = {
                    "work_order_id": work_order_id,
                    "slot_type_cd": slot_type_cd
                }

                # Choose SQL query based on table_name parameter
                if table_name.upper() == "PEN_SLOT_FACT":
                    query = f"""
                        SELECT psf.pn_id, {column_name}
                        FROM {self._schema}.{table_name} psf
                        JOIN {self._schema}.SLOT_DIM sd ON sd.slot_dim_ky = psf.slot_dim_ky
                        WHERE WORK_ORDER_ID = %(work_order_id)s
                        AND COALESCE(sd.SLOT_TYPE_CD, 'NULL') = COALESCE(%(slot_type_cd)s, 'NULL')
                    """
                else:
                    query = f"""
                        SELECT psf.pn_id, {column_name}
                        FROM RPTDS.PEN_SLOT_FACT psf
                        JOIN {self._schema}.{table_name} pncf ON pncf.pn_id = psf.pn_id
                        JOIN {self._schema}.SLOT_DIM sd ON sd.slot_dim_ky = psf.slot_dim_ky 
                        WHERE psf.WORK_ORDER_ID = %(work_order_id)s
                        AND COALESCE(sd.SLOT_TYPE_CD, 'NULL') = COALESCE(%(slot_type_cd)s, 'NULL')
                    """

                # Apply additional filtering based on table type
                if prod_color_dim_ky is not None:
                    query += f" AND psf.PROD_COLOR_DIM_KY = %(prod_color_dim_ky)s"
                    params["prod_color_dim_ky"] = prod_color_dim_ky
                else:
                    if filters_prod_colors:
                        for i, color in enumerate(filters_prod_colors):
                            key = f"prod_color_dim_ky_{i}"
                            query += f" AND NOT psf.PROD_COLOR_DIM_KY = %({key})s"
                            params[key] = color

                query += f" AND {column_name} IS NOT NULL ORDER BY {column_name} DESC"

                self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                return cursor.fetchall()
                
                # Debug: Log the raw data to identify null issues
                # self.logger.debug(f"Raw query returned {len(rows)} rows for column {column_name}")
                # if rows:
                #     self.logger.debug(f"First few raw values: {rows[:5]}")
                #     # Check for data type issues
                #     non_null_count = sum(1 for row in rows if row[0] is not None)
                #     self.logger.debug(f"Non-null values: {non_null_count}/{len(rows)}")
                    
                #     # Check for specific problematic values
                #     for i, row in enumerate(rows[:10]):  # Check first 10 rows
                #         if row[0] is None:
                #             self.logger.warning(f"Row {i} contains None value despite IS NOT NULL filter")
                #         elif str(row[0]).lower() in ['null', '', 'none']:
                #             self.logger.warning(f"Row {i} contains string null-like value: '{row[0]}'")
                
                # return rows

    def get_dates_to_failure(self, work_order_id, days_to_process_wo_ct):
        self.logger.info(
            f"get_dates_to_failure() for work order id: {work_order_id}, "
            f"days_to_process_wo_ct: {days_to_process_wo_ct}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT CASE WHEN MAX_DATE.CALENDAR_DT < CURRENT_DATE THEN 1 ELSE 0 END AS CASE
                    FROM (
                        SELECT CALENDAR_DT
                        FROM (
                            SELECT *
                            FROM (
                                SELECT D.*
                                FROM {self._schema}.DATE_DAY_DIM D,
                                    (
                                        SELECT DATE_TRUNC('day', WORK_ORDER_CLOSE_DM) AS CLOSE_DM
                                        FROM {self._schema}.WORK_ORDER_DIM
                                        WHERE WORK_ORDER_ID = %(work_order_id)s
                                    ) C
                                WHERE D.CALENDAR_DT >= C.CLOSE_DM
                                AND D.HP_US_WORKDAY_OF_MONTH_NR IS NOT NULL
                                AND D.HP_US_HOLIDAY_FG != 'Y'
                                ORDER BY D.CALENDAR_DT ASC
                                LIMIT %(days_to_process_wo_ct)s
                            )
                            ORDER BY CALENDAR_DT DESC
                            LIMIT 1
                        )
                    ) MAX_DATE
                """

                params = {
                    "work_order_id": work_order_id,
                    "days_to_process_wo_ct": days_to_process_wo_ct
                }

                #self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                return cursor.fetchone()

    def store_wo_result_falconDB(self, work_order_id, result):
        self.logger.info(
            f"store_wo_result_falconDB() for work order id: {work_order_id}, result: {result}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Assuming a PostgreSQL sequence exists
                query = f"""
                    INSERT INTO {self._schema}.FCEOLQT_WO_RESULT_FACT 
                    (FCEOLQT_WORSLT_KY, WORK_ORDER_ID, STATUS_CD, EMAIL_SENT_FG, LATEST_RUN_DM)
                    VALUES (nextval('{self._schema}.FCEOLQT_WO_RESULT_SEQ'), %(work_order_id)s, %(result)s, 'N', CURRENT_TIMESTAMP)
                """

                self.logger.info(
                    f"{query} (params: work_order_id = {work_order_id}, result = {result})"
                )

                if not self._should_insert:
                    self.logger.info("Not issuing insert because should_insert is False")

                if self._should_insert:
                    cursor.execute(query, {"work_order_id": work_order_id, "result": result})
                    connection.commit()

    def block_work_orders(self, work_orders):
        work_order_ids = [(x.id,) for x in work_orders]
        self.logger.info(f"block_work_orders() for work order ids: {work_order_ids}")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    INSERT INTO {self._schema}.FCEOLQT_WO_RESULT_FACT
                    (FCEOLQT_WORSLT_KY, WORK_ORDER_ID, STATUS_CD, EMAIL_SENT_FG, LATEST_RUN_DM)
                    VALUES (nextval('{self._schema}.FCEOLQT_WO_RESULT_SEQ'), %s, 'B', 'N', CURRENT_TIMESTAMP)
                """

                #self.logger.info(f"{query} (params: {work_order_ids})")

                if not self._should_insert:
                    self.logger.info("Not issuing insert because should_insert is False")
                    return

                cursor.executemany(query, work_order_ids)
                connection.commit()

    def set_email_sent(self, work_orders):
        work_orders_id = [work_order.id for work_order in work_orders]
        self.logger.info(f"set_email_sent() for work order ids: {work_orders_id}")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                for work_order in work_orders:
                    # Get the most recent result record
                    query = f"""
                        SELECT FCEOLQT_WORSLT_KY
                        FROM {self._schema}.FCEOLQT_WO_RESULT_FACT 
                        WHERE WORK_ORDER_ID = %(work_order_id)s
                        ORDER BY LATEST_RUN_DM DESC
                    """
                    self.logger.info(
                        f"{query} (params: work_order_id = {work_order.id})"
                    )

                    cursor.execute(query, {"work_order_id": work_order.id})
                    result = cursor.fetchone()

                    if result:
                        fceolqt_worslt_ky = result[0]
                        update_query = f"""
                            UPDATE {self._schema}.FCEOLQT_WO_RESULT_FACT
                            SET EMAIL_SENT_FG = 'Y'
                            WHERE FCEOLQT_WORSLT_KY = %(fceolqt_worslt_ky)s
                        """

                        #self.logger.info(
                        #    f"{update_query} (params: fceolqt_worslt_ky = {fceolqt_worslt_ky})"
                        #)

                        if not self._should_update:
                            self.logger.info(
                                "Not issuing update because should_update is False"
                            )
                            continue

                        cursor.execute(update_query, {"fceolqt_worslt_ky": fceolqt_worslt_ky})
                        connection.commit()

   


    def update_wo_test_fact_falconDB(self, work_order_id, constraint_key, pass_fail_result, prod_color_dim_ky=None):
        self.logger.info(
            f"update_wo_test_fact_falconDB() for work order id: {work_order_id}, "
            f"constraint_key: {constraint_key}, result: {pass_fail_result}, prod_color_dim_ky: {prod_color_dim_ky}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                check_query = f"""
                    SELECT COUNT(FCEOLQT_WO_TEST_CNSTR_DIM_KY)
                    FROM {self._schema}.FCEOLQT_WO_TEST_FACT
                    WHERE WORK_ORDER_ID = %(work_order_id)s
                    AND FCEOLQT_WO_TEST_CNSTR_DIM_KY = %(constraint_key)s
                    AND prod_color_dim_ky = %(prod_color_dim_ky)s
                """
                params = {
                    "work_order_id": work_order_id,
                    "constraint_key": constraint_key,
                    "pass_fail_result": pass_fail_result,
                    "prod_color_dim_ky": prod_color_dim_ky  # Use the provided prod_color_dim_ky value
                }

                self.logger.info(f"{check_query} (params: {params})")
                cursor.execute(check_query, params)
                exist = cursor.fetchone()
                self.logger.info(f"exist count: {exist[0]}")

                if exist[0] == 0:
                    update_query = f"""
                        INSERT INTO {self._schema}.FCEOLQT_WO_TEST_FACT 
                        (WORK_ORDER_ID, FCEOLQT_WO_TEST_CNSTR_DIM_KY, PASS_FAIL_FG, LATEST_RUN_DM, PROD_COLOR_DIM_KY)
                        VALUES (%(work_order_id)s, %(constraint_key)s, %(pass_fail_result)s, CURRENT_TIMESTAMP, %(prod_color_dim_ky)s)
                    """
                else:
                    update_query = f"""
                        UPDATE {self._schema}.FCEOLQT_WO_TEST_FACT
                        SET PASS_FAIL_FG = %(pass_fail_result)s, LATEST_RUN_DM = CURRENT_TIMESTAMP
                        WHERE WORK_ORDER_ID = %(work_order_id)s
                        AND FCEOLQT_WO_TEST_CNSTR_DIM_KY = %(constraint_key)s
                        AND prod_color_dim_ky = %(prod_color_dim_ky)s
                    """

                #self.logger.info(f"{update_query} (params: {params})")

                if not self._should_update or not self._should_insert:
                    self.logger.info(
                        "Not issuing update/insert because should_update and should_insert are False"
                    )
                    return

                cursor.execute(update_query, params)
                connection.commit()


    def get_delta_e_coordinates_for_slot_type_cd(
        self, work_order_id, slot_type_cd, prod_color_dim_ky, pen_ids=None
    ):
        self.logger.info(
            f"get_delta_e_coordinates_for_slot_type_cd() for work order id: {work_order_id}, "
            f"prod_color_dim_ky: {prod_color_dim_ky}, slot type cd: {slot_type_cd}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Initialize params dictionary
                params = {
                    "work_order_id": work_order_id,
                    "slot_type_cd": slot_type_cd,
                    "prod_color_dim_ky": prod_color_dim_ky
                }
                
                # Handle empty list
                if not pen_ids:
                    self.logger.info("No pen IDs provided, returning empty result")
                    return []
                
                # Create named placeholders for IN clause and add pen_ids to params
                pen_id_placeholders = []
                for i, pen_id in enumerate(pen_ids):
                    placeholder_key = f"pen_id_{i}"
                    pen_id_placeholders.append(f"%({placeholder_key})s")
                    params[placeholder_key] = pen_id
                
                placeholders = ', '.join(pen_id_placeholders)

                query = f"""
                WITH wo_current AS (
                    SELECT DISTINCT
                        psf.work_order_id,
                        wod.work_order_close_dm,
                        psf.arch_id,
                        psf.even_ink_color_nm,
                        psf.odd_ink_color_nm,
                        psf.odd_ink_type_nm,
                        pfd.prod_group_dim_ky
                    FROM rptds.pen_slot_fact psf
                    JOIN rptds.work_order_dim wod
                    ON wod.work_order_id = psf.work_order_id
                    JOIN rptds.product_family_dim pfd
                    ON pfd.prod_family_dim_ky = wod.prod_family_dim_ky
                    WHERE psf.work_order_id = %(work_order_id)s
                    LIMIT 1
                )
                select
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY psf.hue2_astar_av) AS median_a,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY psf.hue2_bstar_av) AS median_b,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY psf.hue2_lstar_av) AS median_l
                FROM rptds.pen_slot_fact psf
                JOIN rptds.work_order_dim wod
                ON wod.work_order_id = psf.work_order_id
                JOIN rptds.product_family_dim pfd
                ON pfd.prod_family_dim_ky = wod.prod_family_dim_ky
                JOIN rptds.slot_dim sd
                ON sd.slot_dim_ky = psf.slot_dim_ky
                CROSS JOIN wo_current wc
                WHERE psf.work_order_id <> wc.work_order_id
                AND psf.even_ink_color_nm = wc.even_ink_color_nm
                AND psf.odd_ink_color_nm  = wc.odd_ink_color_nm
                AND psf.odd_ink_type_nm   = wc.odd_ink_type_nm
                AND pfd.prod_group_dim_ky = wc.prod_group_dim_ky
                AND wod.work_order_status_nm = 'Closed'
                AND wod.work_order_dest_nm   = 'FGI'
                AND psf.hue2_test_dm     IS NOT NULL
                AND COALESCE(sd.slot_type_cd, 'NULL') = COALESCE(%(slot_type_cd)s, 'NULL')
                AND psf.prod_color_dim_ky = %(prod_color_dim_ky)s
                AND wod.work_order_close_dm < (clock_timestamp() - interval '15 days')
                LIMIT 30;
                """

                #self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)
                
                result = cursor.fetchone()
                if result and all(coord is not None for coord in result):
                    # Return in same format as Oracle: list of tuples
                    return [(float(result[0]), float(result[1]), float(result[2]))]
                else:
                    # Return empty list when no data found (same as Oracle)
                    return []

    def get_pens_per_slot_type_cd(self, work_order_id, slot_type_cd, prod_color_dim_ky=None, pen_ids=None):
        self.logger.info(
            f"get_pens_per_slot_type_cd() for work order id: {work_order_id}, slot type cd: {slot_type_cd}, prod color dim ky: {prod_color_dim_ky}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Initialize params dictionary
                params = {
                    "work_order_id": work_order_id,
                    "slot_type_cd": slot_type_cd,
                    "prod_color_dim_ky": prod_color_dim_ky
                }
                
                # Handle empty list
                if not pen_ids:
                    self.logger.info("No pen IDs provided, returning empty result")
                    return []
                
                # Create named placeholders for IN clause and add pen_ids to params
                pen_id_placeholders = []
                for i, pen_id in enumerate(pen_ids):
                    placeholder_key = f"pen_id_{i}"
                    pen_id_placeholders.append(f"%({placeholder_key})s")
                    params[placeholder_key] = pen_id
                
                placeholders = ', '.join(pen_id_placeholders)
                query = f""" 
                    SELECT PSF.*
                    FROM {self._schema}.PEN_SLOT_FACT PSF 
                    JOIN {self._schema}.SLOT_DIM SD ON SD.SLOT_DIM_KY = PSF.SLOT_DIM_KY
                    WHERE 
                    PSF.PN_ID in ({placeholders}) 
                    AND PSF.PROD_COLOR_DIM_KY = %(prod_color_dim_ky)s
                    AND PSF.HUE2_TEST_DM IS NOT NULL
                    AND COALESCE(SD.SLOT_TYPE_CD, 'NULL') = COALESCE(%(slot_type_cd)s, 'NULL')
                """

                #self.logger.info(f"{query} (params: {params})")
                cursor.execute(query, params)

                return cursor.fetchall()


    def get_test_types(self, arch_id, ink_type_dim_ky):
        # self.logger.info(
        #     f"get_test_types() for arch id: {arch_id} and ink type dim ky: {ink_type_dim_ky}"
        # )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT FCEOLQT_WO_TYPE_DIM_KY, WORK_ORDER_TYPE_NM, INK_TYPE_DIM_KY, ARCH_ID,
                        MIN_PEN_CT, DAYS_TO_PROCESS_WO_CT, ACTIVE_FG
                    FROM RPTDS.FCEOLQT_WO_TYPE_DIM
                    WHERE arch_id::int IS NOT DISTINCT FROM %(arch_id)s::int
                    AND ink_type_dim_ky IS NOT DISTINCT FROM %(ink_type_dim_ky)s::int AND ACTIVE_FG = 'Y'
                """

                # self.logger.info(
                #      f"{query} (params: {{'arch_id': {arch_id}, 'ink_type_dim_ky': {ink_type_dim_ky}}})"
                # )

                cursor.execute(query, {
                    "arch_id": arch_id,
                    "ink_type_dim_ky": ink_type_dim_ky
                })

                rows = cursor.fetchall()
                # Use generic conversion method for tuples
                return self._convert_decimal_columns(rows)


    def update_delta_e(self, pn_id, die_site_nr, slot_dim_ky, delta_e):
        # self.logger.info(
        #     f"update_delta_e() for pn_id: {pn_id}, die_site_nr: {die_site_nr}, "
        #     f"slot_dim_ky: {slot_dim_ky}, delta_e: {delta_e}"
        # )
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    UPDATE {self._schema}.PEN_SLOT_FACT
                    SET DELTA_E_VL_2 = %(delta_e)s
                    WHERE 
                        PN_ID = %(pn_id)s AND 
                        DIE_SITE_NR = %(die_site_nr)s AND
                        SLOT_DIM_KY = %(slot_dim_ky)s
                """

                # self.logger.info(
                #     f"{query} (params: pn_id = {pn_id}, die_site_nr = {die_site_nr}, "
                #     f"slot_dim_ky = {slot_dim_ky}, delta_e = {delta_e})"
                # )

                if not self._should_update:
                    self.logger.info("Not issuing update because should_update is False")
                    return

                cursor.execute(query, {
                    "pn_id": pn_id,
                    "die_site_nr": die_site_nr,
                    "slot_dim_ky": slot_dim_ky,
                    "delta_e": delta_e
                })
                connection.commit()

    def rptds_func_fceolqt_upd_pen_noz_clous_fact(self, wo_id):
        self.logger.info(
                    f"Calling rptds_func_fceolqt_upd_pen_noz_clous_fact() for wo_id: {wo_id}"
                )
        query  = f'SELECT rptds."fceolqt$upd_pen_noz_col_clous_fact"(%(wo_id)s)'

        with self.get_connection() as connection:
            connection.autocommit = False
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, {"wo_id": wo_id})
                    ret = cursor.fetchone()
                    result =  int(ret[0]) if ret else None
                    self.logger.info(f"{query} for wo_id: {wo_id} returned: {ret}")
                    self.logger.info(f"wo_id sent to DB: {wo_id!r}")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return result

    def rptds_func_fceolqt_upd_pen_slot_clous_fact(self, wo_id):
        self.logger.info(
            f"SELECT rptds_func_fceolqt_upd_pen_slot_clous_fact() for wo_id: {wo_id}"
        )
        query = f'SELECT {self._schema}."fceolqt$upd_pen_slot_clous_fact"(%(wo_id)s)'
       
        with self.get_connection() as connection:
            connection.autocommit = False
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, {"wo_id": wo_id})
                    ret = cursor.fetchone()
                    result = ret[0] if ret else None
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return result

    def rptds_func_fceolqt_upd_pen_slot_hue_2_fact(self, wo_id):
        self.logger.info(
            f"SELECT rptds_func_fceolqt_upd_pen_slot_hue_2_fact() for wo_id: {wo_id}"
        )
        query = f'SELECT {self._schema}."fceolqt$upd_pen_slot_hue_2_fact"(%(wo_id)s)'
    
        with self.get_connection() as connection:
            connection.autocommit = False
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, {"wo_id": wo_id})
                    ret = cursor.fetchone()
                    result = ret[0] if ret else None
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return result if result else 0

    def rptds_func_fceolqt_calc_delta_e_for_slottypecd_2(self, wo_id, slot_type_cd=None):
        self.logger.info(
            f"SELECT rptds_func_fceolqt_calc_delta_e_for_slottypecd_2() for wo_id: {wo_id}, slot_type_cd: {slot_type_cd}"
        )
        query = f'SELECT rptds."fceolqt$calc_delta_e_for_slottypecd_2"(%(wo_id)s::text, %(slot_type_cd)s::text)'
        result = None
        with self.get_connection() as connection:
            connection.autocommit = False
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, {"wo_id": str(wo_id), "slot_type_cd": str(slot_type_cd) if slot_type_cd is not None else ""})
                    result = cursor.fetchone()
                connection.commit()
            except Exception:
                connection.rollback()
                
        return result[0] if result else 0
            
    def calc_percentile_using_stored_function(self, data, pct):
        self.logger.info(
            f"calc_percentile_using_stored_function() for pct: {pct}, data length: {len(data)}"
        )

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Log the function call
                self.logger.info(f"Calling PostgreSQL function {self._schema}.FCEOLQT$CALC_PERCENTILE")

                # PostgreSQL expects arrays as native Python lists
                query = f"SELECT {self._schema}.FCEOLQT$CALC_PERCENTILE(%(data)s, %(pct)s)"
                cursor.execute(query, {"data": data, "pct": pct})
                result = cursor.fetchone()[0]

                self.logger.info(f"Result from PostgreSQL: {result}")
                return result




    def get_failure_reasons(self, work_order_id):
        self.logger.info(f"get_failure_reasons() for work order id: {work_order_id}")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT TCRI.TEST_CRITERIA_NM, PC.PROD_COLOR_NM
                    FROM {self._schema}.FCEOLQT_WO_TEST_FACT TF 
                        INNER JOIN {self._schema}.FCEOLQT_WO_TEST_CNSTR_DIM TCNSTR
                            ON TCNSTR.FCEOLQT_WO_TEST_CNSTR_DIM_KY = TF.FCEOLQT_WO_TEST_CNSTR_DIM_KY
                        INNER JOIN {self._schema}.FCEOLQT_TEST_CRITERIA_DIM TCRI
                            ON TCNSTR.FCEOLQT_TEST_CRITERIA_DIM_KY = TCRI.FCEOLQT_TEST_CRITERIA_DIM_KY
                        LEFT OUTER JOIN {self._schema}.PRODUCT_COLOR_DIM PC 
                            ON TCNSTR.PROD_COLOR_DIM_KY = PC.PROD_COLOR_DIM_KY
                    WHERE TF.WORK_ORDER_ID = %(work_order_id)s
                        AND TF.PASS_FAIL_FG = 'F'
                    ORDER BY TCNSTR.FCEOLQT_WO_TEST_CNSTR_DIM_KY
                """

                self.logger.info(f"{query} (params: work_order_id = {work_order_id})")
                cursor.execute(query, {"work_order_id": work_order_id})
                return cursor.fetchall()


    def get_work_orders_with_no_email_sent(self, tested_work_order_ids):
        self.logger.info("get_work_orders_with_no_email_sent()")
        results = []

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                tested_work_order_ids_query = ""
                parameters = {}

                if tested_work_order_ids:
                    placeholders = []
                    for index, wo_id in enumerate(tested_work_order_ids):
                        key = f"tested_work_order_id_{index}"
                        placeholders.append(f"%({key})s")
                        parameters[key] = wo_id
                    tested_work_order_ids_query = f"AND NOT WORK_ORDER_ID IN ({', '.join(placeholders)})"

                # Main query to get work_order_ids
                query = f"""
                    SELECT DISTINCT WORK_ORDER_ID
                    FROM (
                        SELECT * 
                        FROM {self._schema}.FCEOLQT_WO_RESULT_FACT
                        WHERE EMAIL_SENT_FG = 'N'
                        AND NOT WORK_ORDER_ID IN (
                            SELECT WORK_ORDER_ID 
                            FROM {self._schema}.FCEOLQT_WO_RESULT_FACT
                            WHERE EMAIL_SENT_FG = 'Y'
                        )
                        {tested_work_order_ids_query}
                    ) AS pending
                """

                self.logger.info(f"{query} (params: {parameters})")
                cursor.execute(query, parameters)

                for row in cursor.fetchall():
                    work_order_id = row[0]

                    # Get latest status for this work order
                    status_query = f"""
                        SELECT STATUS_CD 
                        FROM (
                            SELECT * 
                            FROM {self._schema}.FCEOLQT_WO_RESULT_FACT WOR
                            WHERE WOR.WORK_ORDER_ID = %(work_order_id)s
                            ORDER BY WOR.LATEST_RUN_DM DESC
                        ) AS ordered_results
                        LIMIT 1
                    """

                    self.logger.info(f"{status_query} (params: work_order_id = {work_order_id})")
                    cursor.execute(status_query, {"work_order_id": work_order_id})
                    result = cursor.fetchone()

                    if result:
                        results.append((work_order_id, result[0]))

        return results

    def get_color_keys(self, prod_color_nm):
        self.logger.info(
            f"""get_color_keys() for prod_color_nm: {prod_color_nm}"""
        )
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                # Handle tuple of color names for IN clause
                if isinstance(prod_color_nm, (tuple, list)):
                    # Create placeholders for IN clause
                    placeholders = ', '.join(['%s' for _ in range(len(prod_color_nm))])
                    # Create parameter tuple
                    params = tuple(prod_color_nm)
                else:
                    placeholders = '%s'
                    params = (prod_color_nm,)
                
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

    def get_pool(self):
        return self._pool