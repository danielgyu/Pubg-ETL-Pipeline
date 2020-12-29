from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator               import BaseOperator
from airflow.utils.decorators                  import apply_defaults

class DataQualityCheckOperator(BaseOperator):
    """
    """

    check_data_sql = (
        """
        SELECT
            CAST (
                SUM ( CASE WHEN {} IS null THEN 1 ELSE 0 END ) as FLOAT
            ) / COUNT(*) as data_missing
        FROM {}
        """)

    @apply_defaults
    def __init__(self,
                 postgres_conn_id=None,
                 tables=None,
                 columns=None,
                 expected_result=None,
                 *args, **kwargs):
        """
        """

        super().__init__(*args, **kwargs)
        self.redshift_conn   = postgres_conn_id
        self.tables          = tables
        self.columns         = columns
        self.expected_result = expected_result

    def execute(self, context):
        """
        """
        checks = zip(self.tables, self.columns)

        for check in checks:
            sql = DataQualityCheckOperator.check_data_sql.format(
                check[1],
                check[0]
            )

            try:
                redshift_hook = PostgresHook(self.redshift_conn)
                redshift_hook.run(sql) == self.expected_result
                self.log.info('PASSED : {} from {}'.format(check[0], check[1]))
            except:
                raise ValueError('FAILED : {} from {}'.format(check[0], check[1]))

