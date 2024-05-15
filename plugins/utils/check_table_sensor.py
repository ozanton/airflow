from airflow.models.connection import Connection
from airflow.sensors.base import BaseSensorOperator
import psycopg2 as pg

class CheckTableSensor(BaseSensorOperator):
    poke_context_fields = ['conn', 'table_name']

    def __init__(self, conn: Connection, table_name: str, *args, **kwargs):
        self.conn = conn
        self.table_name = table_name
        super(CheckTableSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        try:
            connection = pg.connect(f"host={self.conn.host} dbname={self.conn.schema} user={self.conn.login} password={self.conn.password}")
            cursor = connection.cursor()
            cursor.execute(f'SELECT count(*) FROM {self.table_name}', connection)
            result = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            if result > 0:
                self.log.info(f"УРА!!! В {self.table_name} есть данные.")
                return True
            else:
                self.log.info(f"Печалька, в {self.table_name} данных нет.")
                return False
        except Exception as e:
            self.log.error(f"Error checking table {self.table_name}: {e}")
            return False