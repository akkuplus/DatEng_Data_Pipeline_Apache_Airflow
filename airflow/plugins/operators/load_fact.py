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

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.do_append:
            redshift.run("DELETE FROM {}".format(self.table))         
            self.log.info("Truncated values from {} table".format(self.table))

        rendered_sql = getattr(SqlQueries, self.query_name).format(self.table)
        insert_query = "INSERT INTO {}  ({})".format(self.table, rendered_sql)
        self.log.warning("Formatted SQL {}".format(insert_query))
        
        self.log.info("Inserting data from staging tables into {} table".format(self.table))
        redshift.run(insert_query)
        self.log.info("Inserted data from staging tables into {} table".format(self.table))
        
        return
