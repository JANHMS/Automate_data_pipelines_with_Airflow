from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    """
    This is used for data quality checks on the tables.
    Parameters
    ----------
    - BaseOberator: Airflow Operator
    Returns:
    --------
    logger message
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.tables = tables


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            # records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            # do not hardcode, but instead my dynamic.
            checks = [
                {'test_sql': f'SELECT COUNT(*) FROM {table}', 'expected_result': 0, 'comparison': '==', 'element_comparison': 'records[0][0]', 'explanation error' : ' contained 0 rows'},
            ]

            for i, check in enumerate(checks):
                records = redshift_hook.get_records(check['test_sql'])

                if not f"{check['expected_result']} {check['comparison']} {check['element_comparison']} ":
                    raise ValueError(f"Data quality #{i} failed. {table} {check['explanation error']}.")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
