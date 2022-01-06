from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 query_name = "",        
                 do_append = False,
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query_name = query_name
        self.do_append = do_append
        
        return

    def execute(self, context):
        """
        Load data from staging table into fact table.
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.do_append:
            redshift.run("DELETE FROM {}".format(self.table))         
            self.log.info("Truncated values from {} table".format(self.table))

        try:
            query = getattr(SqlQueries, self.query_name).format(self.table) # Pay attention to the attribute self.query_name! It contains the query.
            query = "INSERT INTO {}  ({})".format(self.table, query)
            self.log.info("Formatted SQL: {}".format(query))
        except Exception as ex:
            self.log.error("Error rendering query. Resolved query_name {}".format(self.query_name))
        
        self.log.info("Inserting data from staging table(s) into {} table".format(self.table))
        redshift.run(query)
        self.log.info("Inserted data from staging table(s) into {} table".format(self.table))
        
        return
