from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator               import BaseOperator
from airflow.utils.decorators                  import apply_defaults

class LoadWarehouseOperator(BaseOperator):
    """
    Loads the corresponding Redshift table with input parameters.
    """

    sql = ("""
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS parquet
           """)

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = None,
                 aws_access_key   = None,
                 aws_secret_key   = None,
                 s3_bucket        = None,
                 s3_key           = None,
                 table            = None,
                 truncate         = False,
                 *args, **kwargs):
        """
        Parmas
        ------
        postgres_conn_id : str
            connection name specified in Airflow Connections
        aws_access_key : str
            access key id for AWS credential
        aws_secret_key : str
            secret access key for AWS credential
        s3_bucket : str
            bucket name for data transfer
        s3_key : str
            specific key name of the corresponding table data
        table : str
            table name in Redshift
        truncate : bool
            option to truncate the table before loading
        """

        super().__init__(*args, **kwargs)
        self.redshift_conn = postgres_conn_id
        self.table         = table
        self.s3_bucket     = s3_bucket
        self.s3_key        = s3_key
        self.access_key    = aws_access_key
        self.secret_key    = aws_secret_key
        self.truncate      = truncate

    def execute(self, context):
        """
        Designates target S3 ARN by combining parameter inputs.
        Then makes a connection to redshift, and loads the table.
        """

        s3_arn = "{}{}".format(self.s3_bucket, self.s3_key)

        sql = LoadWarehouseOperator.sql.format(
            self.table,
            s3_arn,
            self.access_key,
            self.secret_key,
        )
        redshift_hook = PostgresHook(self.redshift_conn)
        if self.truncate:
            redshift_hook.run('TRUNCATE TABLE {}'.format(self.table))
        redshift_hook.run(sql)

