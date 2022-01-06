from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime
from dateutil import parser

class StageToRedshiftOperator(BaseOperator):
    """
        Operator loads JSON files from AWS S3 <path> into AWS Redshift <table>.
    """

    ui_color = '#358140'

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        """

    TRUNCATE_SQL = """
        TRUNCATE TABLE {};
        """
    
    template_fields = ['execution_date']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table_name="",
                 s3_bucket="",
                 json_path="auto",
                 execution_date="",
                 do_append = False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.json_path = json_path
        self.execution_date = execution_date
        self.do_append = do_append
        
        return

    def execute(self, context):

        s3_path = self.s3_bucket
        self.log.info(f"S3 path: {s3_path}")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Truncate table
        if not self.do_append:
            self.log.info(f"Truncating table {self.table_name}")
            redshift_hook.run(self.TRUNCATE_SQL.format(self.table_name))    
        
        prepared_statement = self.COPY_SQL.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )

        redshift_hook.run(prepared_statement)
        self.log.info(f"Copied into table {self.table_name}")
        
        return