"""
Query adapter to handle database-specific SQL parameter syntax differences.
This allows shared SQL queries to work with both Oracle and PostgreSQL providers.
"""
import re
from typing import Dict, Any, Tuple


class QueryAdapter:
    """
    Handles conversion of SQL queries and parameters between different database engines.
    
    Oracle format:  :param_name with cursor.execute(query, param_name=value)
    PostgreSQL format: %(param_name)s with cursor.execute(query, {"param_name": value})
    """
    
    @staticmethod
    def oracle_to_postgres(query: str, **kwargs) -> Tuple[str, Dict[str, Any]]:
        """
        Convert Oracle-style query and parameters to PostgreSQL format.
        
        Args:
            query: SQL query with Oracle-style :param placeholders
            **kwargs: Parameter values as keyword arguments
            
        Returns:
            Tuple of (postgres_query, param_dict)
        """
        # Convert :param_name to %(param_name)s
        postgres_query = re.sub(r':(\w+)', r'%(\1)s', query)
        param_dict = kwargs
        return postgres_query, param_dict
    
    @staticmethod
    def postgres_to_oracle(query: str, param_dict: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Convert PostgreSQL-style query and parameters to Oracle format.
        
        Args:
            query: SQL query with PostgreSQL-style %(param)s placeholders
            param_dict: Parameter values as dictionary
            
        Returns:
            Tuple of (oracle_query, param_dict) - params remain as dict for Oracle
        """
        # Convert %(param_name)s to :param_name
        oracle_query = re.sub(r'%\((\w+)\)s', r':\1', query)
        return oracle_query, param_dict
    
    @staticmethod
    def create_universal_query(base_query: str) -> 'UniversalQuery':
        """
        Create a universal query object that can be executed on any database.
        Use Oracle format as the base since it's simpler (:param vs %(param)s).
        """
        return UniversalQuery(base_query)


class UniversalQuery:
    """
    A query that can be executed on multiple database engines with automatic parameter conversion.
    """
    
    def __init__(self, oracle_query: str):
        """Initialize with Oracle-style query (:param format)."""
        self.oracle_query = oracle_query
        self._postgres_query = None
    
    @property 
    def postgres_query(self) -> str:
        """Get PostgreSQL version of the query."""
        if self._postgres_query is None:
            self._postgres_query = re.sub(r':(\w+)', r'%(\1)s', self.oracle_query)
        return self._postgres_query
    
    def execute_oracle(self, cursor, **kwargs):
        """Execute on Oracle database with keyword parameters."""
        cursor.execute(self.oracle_query, **kwargs)
        return cursor.fetchall()
    
    def execute_postgres(self, cursor, **kwargs):
        """Execute on PostgreSQL database with dictionary parameters.""" 
        cursor.execute(self.postgres_query, kwargs)
        return cursor.fetchall()


# Common SQL queries that can be shared between providers
class SharedQueries:
    """
    Collection of database-agnostic SQL queries using Oracle parameter format as base.
    These will be automatically converted for PostgreSQL usage.
    """
    
    @staticmethod
    def get_constraints(schema: str) -> UniversalQuery:
        query = f"""
            SELECT FCEOLQT_WO_TEST_CNSTR_DIM_KY, FCEOLQT_TEST_CRITERIA_DIM_KY, PROD_COLOR_DIM_KY, 
                CONSTRAINT_UPPER_BOUND_VL, CONSTRAINT_LOWER_BOUND_VL, CONSTRAINT_CENTILE_PCT,  
                SLOT_TYPE_CD, ACTIVE_FG 
            FROM {schema}.FCEOLQT_WO_TEST_CNSTR_DIM
            WHERE FCEOLQT_WO_TYPE_DIM_KY = :test_type_key AND ACTIVE_FG = 'Y'
        """
        return UniversalQuery(query)
    
    @staticmethod
    def get_test_criteria(schema: str) -> UniversalQuery:
        query = f"""
            SELECT TABLE_NM, COLUMN_NM, TEST_CRITERIA_NM 
            FROM {schema}.FCEOLQT_TEST_CRITERIA_DIM CRITERIA
            JOIN (
                SELECT FCEOLQT_TEST_CRITERIA_DIM_KY
                FROM {schema}.FCEOLQT_WO_TEST_CNSTR_DIM 
                WHERE FCEOLQT_WO_TEST_CNSTR_DIM_KY = :constraint_key
            ) CONSTRAINT_INFO ON CRITERIA.FCEOLQT_TEST_CRITERIA_DIM_KY = CONSTRAINT_INFO.FCEOLQT_TEST_CRITERIA_DIM_KY
        """
        return UniversalQuery(query)
    
    @staticmethod
    def get_work_order(schema: str) -> UniversalQuery:
        query = f"""
            SELECT wo.WORK_ORDER_ID, wo.ARCH_ID, wo.INK_TYPE_DIM_KY, wo.FCEOLQT_WO_TYPE_DIM_KY, 
                wo.CREATE_TS, wo.UPDATE_TS, wo.TEST_FINISH_TS, wt.WORK_ORDER_TYPE_NM, wt.MIN_PEN_CT, wt.ACTIVE_FG
            FROM {schema}.FCEOLQT_WORK_ORDER_DIM wo
            JOIN {schema}.FCEOLQT_WO_TYPE_DIM wt ON wo.FCEOLQT_WO_TYPE_DIM_KY = wt.FCEOLQT_WO_TYPE_DIM_KY
            WHERE wo.WORK_ORDER_ID = :work_order_id
        """
        return UniversalQuery(query)
    
    @staticmethod
    def get_pen_slot_facts(schema: str) -> UniversalQuery:
        query = f"""
            SELECT psf.FCEOLQT_WO_PEN_SLOT_FCT_KY, psf.PN_ID, psf.DIE_SITE_NR, psf.SLOT_DIM_KY, 
                psf.REF_A_VL, psf.REF_B_VL, psf.REF_L_VL, psf.MEAS_A_VL, psf.MEAS_B_VL, 
                psf.MEAS_L_VL, psf.DELTA_E_VL, sd.SLOT_TYPE_CD, prod.COLOR_NM
            FROM {schema}.FCEOLQT_WO_PEN_SLOT_FCT psf
            JOIN {schema}.SLOT_DIM sd ON psf.SLOT_DIM_KY = sd.SLOT_DIM_KY  
            JOIN {schema}.PROD_COLOR_DIM prod ON psf.PROD_COLOR_DIM_KY = prod.PROD_COLOR_DIM_KY
            JOIN (SELECT PN_ID FROM {schema}.PEN_INFO_DIM WHERE LAST_WORK_ORDER_ID = :work_order_id) wo 
                ON psf.PN_ID = wo.PN_ID
        """
        return UniversalQuery(query)
        
    @staticmethod
    def get_printed_count_for_falcap(schema: str) -> UniversalQuery:
        query = f"""
            SELECT count(*)
            FROM {schema}.PEN_SLOT_FACT psf
            JOIN ( SELECT DISTINCT PN_ID
                FROM {schema}.PEN_INFO_DIM
                WHERE LAST_WORK_ORDER_ID  = :work_order_id) wo on psf.PN_ID = wo.PN_ID
            where psf.cap_clou_test_dm is not null and psf.hue2_test_dm is not null
        """
        return UniversalQuery(query)
    