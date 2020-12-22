from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    template_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} REGION AS '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 table_schema='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 data_format='',
                 *args, **kwargs):

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.table_schema = table_schema
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(f"DROP TABLE IF EXISTS {self.table};")
        self.log.info(f"old {self.table} dropped (if exists)")

        redshift.run(f"CREATE TABLE {self.table} ({self.table_schema});")
        self.log.info(f"{self.table} created")

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        sql = self.template_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.data_format,
            self.region
        )
        redshift.run(sql)
        self.log.info(f"Data has be copied from s3 bucket: {self.s3_bucket}")