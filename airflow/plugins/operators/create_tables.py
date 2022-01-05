from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries


class CreateTablesInRedshiftOperator(BaseOperator):
    """
    Operator creates tables in Redshift
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(CreateTablesInRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
        return


    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        sql_stmt = SqlQueries.create_tables
        redshift_hook.run(sql_stmt)
        self.log.info(f"Creation of tables successful")

        return