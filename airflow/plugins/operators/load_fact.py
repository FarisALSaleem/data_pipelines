from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    template_sql = """
        INSERT INTO {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 table_schema='',
                 sql_statement='',
                 delete_load=False,
                 *args, **kwargs):

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_schema = table_schema
        self.sql_statement = sql_statement
        self.delete_load = delete_load
        super(LoadFactOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_load:
            redshift.run(f"DROP TABLE IF EXISTS {self.table};")
            self.log.info(f"old {self.table} dropped")

        redshift.run(f"CREATE TABLE {self.table} ({self.table_schema});")
        self.log.info(f"{self.table} created")

        sql = self.template_sql.format(self.table, self.sql_statement)
        redshift.run(sql)
        self.log.info(f"Data has be copied to {self.table}")
