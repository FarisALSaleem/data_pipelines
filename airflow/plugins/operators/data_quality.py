from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables='',
                 *args, **kwargs):
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        super(DataQualityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Doing quality check for {table}")
            records = redshift.get_records(f"SELECT COUNT(*) from {table}")
            if records is None or len(records[0]) < 1:
                self.log.info(f"No records present in table {table}")
                raise ValueError(f"No records present in table {table}")




