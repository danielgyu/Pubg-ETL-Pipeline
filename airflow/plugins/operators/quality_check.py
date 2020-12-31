from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator               import BaseOperator
from airflow.utils.decorators                  import apply_defaults

class DataQualityCheckOperator(BaseOperator):
    """
    Checks the data by running queries to the loaded Redshift tables.
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
        Params
        ------
        postgres_conn_id : str
            connection name of Redshift in Airflow Connection
        tables : list
            a list containing names of the tables to be checked
        columns : list
            a list containing name of the columns to be checked
        expected_result : float
            a float to be checked against in validation process
        """

        super().__init__(*args, **kwargs)
        self.redshift_conn   = postgres_conn_id
        self.tables          = tables
        self.columns         = columns
        self.expected_result = expected_result

    def execute(self, context):
        """
        Validates the loaded data by using the sql defined in class.
        It checks whether the target columns(which do not accept nulls) of the designated tables have null values.
        If there are any null values, it would sum up to be bigger than 0.0, which is the expected result value,
        thus raising a ValueError.
        """
        checks = zip(self.tables, self.columns)

        for check in checks:
            sql = DataQualityCheckOperator.check_data_sql.format(
                check[1],
                check[0]
            )

            try:
                redshift_hook = PostgresHook(self.redshift_conn)
                result = redshift_hook.run(sql)
                result == self.expected_result
                self.log.info('PASSED : {}, Table : {}, Column :  {}'.format(result, check[0], check[1]))
            except:
                raise ValueError('FAILED : {} , Table : {}, Column : {}'.format(result, check[0], check[1]))

