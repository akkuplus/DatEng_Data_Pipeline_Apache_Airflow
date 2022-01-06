from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class DataQualityTestOperator(BaseOperator):
    """
    Operator validates tables by executing a sql test query.

    :param str redshift_conn_id: the name of Airflow connection to Redshift
    :param list tables: list of tables names
    :param dict dq_check: Specification of a data quality check, that contains a test query, a comparator and a test value
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 dq_check = [{'check_sql': "SELECT COUNT(*) FROM {}", "comparison":"gt", "test_value": 0}],
                 *args, **kwargs):

        super(DataQualityTestOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_check = dq_check
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)    
        
        for table in self.tables:
            for check_setting in self.dq_check:
                
                try:
                    query = check_setting["check_sql"].format(table)  # Pay attention to the attribute self.dq_check! It contains all test query parameters.
                    self.log.info("Rendered query is '{}'".format(query))
                except Exception as ex:
                    self.log.error("Error rendering query {}".format(check_setting["check_sql"]))
                    raise

                result = int(redshift.get_first(sql=query)[0])
                self.log.info("Result is '{}'".format(result))

                error_message="Data quality check failed. Table '{}' failed check '{}', that returned result '{}'"


                # check greater than
                if check_setting["comparison"] == 'gt':
                    if not(result > check_setting["test_value"]):
                        raise ValueError(error_message.format(table, check_setting, result))

                # check lower than
                elif check_setting["comparison"] == 'lt':
                    if not(result < check_setting["test_value"]):
                        raise ValueError(error_message.format(table, check_setting, result))

                # check equal
                elif check_setting["comparison"] == 'eq':
                    if not(result == check_setting["test_value"]):
                        raise ValueError(error_message.format(table, check_setting, result))

                # check not equal
                elif check_setting["comparison"] == 'ne':
                    if not(result != check_setting["test_value"]):
                        raise ValueError(error_message.format(table, check_setting, result))


                self.log.info("Data quality check succeded. Table '{}' passed the check query '{}' with result '{}'".format(table, query, result))
                
        return

    
"""
class DataQualityTestOperator(BaseOperator):
    ""
    Operator validates tables by executing a sql test query.

    :param str redshift_conn_id: the name of Airflow connection to Redshift
    :param list tables: list of tables names
    :param dict dq_check: Specification of a data quality check, that contains a test query, a comparator and a test value
    ""

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 dq_check = {'check_sql': "SELECT COUNT(*) FROM {}", "comparison":"gt", "test_value": 0},
                 *args, **kwargs):

        super(DataQualityTestOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_check = dq_check
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)    
        
        for table in self.tables:
                
            try:
                query = self.dq_check["check_sql"].format(table)  # Pay attention to the attribute self.dq_check! It contains all test query parameters.
                self.log.info("Rendered query is '{}'".format(query))
            except Exception as ex:
                self.log.error("Error rendering query {}".format(self.dq_check["check_sql"]))
                raise
            
            result = int(redshift.get_first(sql=query)[0])
            self.log.info("Result is '{}'".format(result))
            
            error_message="Data quality check failed. Table '{}' failed check '{}', that returned result '{}'"
            
    
            # check greater than
            if self.dq_check["comparison"] == 'gt':
                if not(result > self.dq_check["test_value"]):
                    raise ValueError(error_message.format(table, self.dq_check, result))

            # check lower than
            elif self.dq_check["comparison"] == 'lt':
                if not(result < self.dq_check["test_value"]):
                    raise ValueError(error_message.format(table, self.dq_check, result))
       
            # check equal
            elif self.dq_check["comparison"] == 'eq':
                if not(result == self.dq_check["test_value"]):
                    raise ValueError(error_message.format(table, self.dq_check, result))

            # check not equal
            elif self.dq_check["comparison"] == 'ne':
                if not(result != self.dq_check["test_value"]):
                    raise ValueError(error_message.format(table, self.dq_check, result))


            self.log.info("Data quality check succeded. Table '{}' passed the check query '{}' with result '{}'".format(table, query, result))
                
        return
"""