from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class DataQualityOperator(BaseOperator):
    """
    Operator checks that the count of rows of a table is at least <min_number_of_rows>.

    :param str redshift_conn_id: the name of Airflow connection to Redshift
    :param list tables: list of tables names
    :param int min_number_of_rows: The minimal count of all rows
    :param str query_name: The actual query that is executed as data quality check
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 min_number_of_rows = 1,
                 query_name = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.min_number_of_rows = min_number_of_rows
        self.query_name = query_name
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            
            try:
                query = getattr(SqlQueries, self.query_name).format(table) # Pay attention to the attribute self.query_name! It contains the query.
                self.log.info("Formatted SQL {}".format(query))

            except Exception as ex:
                self.log.error("Error rendering query. Resolved query_name {}".format(self.query_name))
                raise
            
            records = redshift.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. Table {} returned no result".format(table))
            elif records[0][0] < self.min_number_of_rows:
                raise ValueError("Data quality check failed. Table {} contained too less records".format(table))
            else:
                self.log.info("Data quality check succeded. Table {} check passed with {} records".format(table, records[0][0]))
                
        return
