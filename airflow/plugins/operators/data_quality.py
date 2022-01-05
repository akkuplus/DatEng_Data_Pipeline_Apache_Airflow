from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class DataQualityOperator(BaseOperator):

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
                query = getattr(SqlQueries, self.query_name).format(table)
                #query = "INSERT INTO {}  ({})".format(self.table, query)
                self.log.info("Formatted SQL {}".format(query))
            except Exception as ex:
                self.log.error("Error rendering query. Resolved query_name {}".format(self.query_name))
                query = []
            
            records = redshift.get_records(query)
            #records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. Table {} returned no result".format(table))
            elif records[0][0] < self.min_number_of_rows:
                raise ValueError("Data quality check failed. Table {} contained too less records".format(table))
            else:
                self.log.info("Data quality check succeded. Table {} check passed with {} records".format(table, records[0][0]))
                
        return
