from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date, Boolean, Float, TIMESTAMP, insert, UniqueConstraint
from sqlalchemy.orm import declarative_base
import logging

Base = declarative_base()

class TableNbrs(Base):
    __tablename__ = 'rates_nbrs'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    date = Column(TIMESTAMP, nullable=False)
    code = Column(VARCHAR(10), nullable=False)
    number = Column(Integer, nullable=False)
    parity = Column(Integer, nullable=False)
    exchange_buy = Column(Float, nullable=False)
    exchange_middle = Column(Float, nullable=False)
    exchange_sell = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint('date', 'code', name='uq_date_code_nbrs'),
    )

class Aga1Operator(BaseOperator):
    def __init__(self,
             postgre_conn: Connection,
             table_name: str,
             python_callable=None,
             default_mapping: dict = {},
             log_unmapped_fields: bool = True,
             **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgre_conn = postgre_conn
        self.python_callable = python_callable
        self.table_name = table_name
        self.default_mapping = default_mapping
        self.log_unmapped_fields = log_unmapped_fields
        self.SQLALCHEMY_DATABASE_URI = f"postgresql://{postgre_conn.login}:{postgre_conn.password}@{postgre_conn.host}:{str(postgre_conn.port)}/{postgre_conn.schema}"

    def execute(self, context):
        engine = create_engine(self.SQLALCHEMY_DATABASE_URI)
        Base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session_local = SessionLocal()

        if self.python_callable:
            try:
                records_to_insert = self.python_callable(**context)
            except Exception as e:
                logging.error(f"Ошибка при выполнении python_callable: {e}")
                return
        else:
            xcom_value = context['task_instance'].xcom_pull(key='data_nbrs')

            if not xcom_value:
                logging.error("Ошибка: Нет данных в XCom.")
                return
            else:
                logging.info("Данные из XCom получены успешно.")

            try:
                records_to_insert = xcom_value
                logging.info(f"Данные для вставки: {records_to_insert}")
                if not isinstance(records_to_insert, list):
                    raise ValueError("Ожидался список записей для вставки")
                for record in records_to_insert:
                    if not isinstance(record, dict):
                        raise ValueError("Ожидался словарь для каждой записи")
                    required_keys = ['date', 'code', 'num', 'parity', 'exchange_buy', 'exchange_middle',
                                     'exchange_sell']
                    missing_keys = [key for key in required_keys if key not in record]
                    if missing_keys:
                        raise ValueError(f"Отсутствуют обязательные ключи в записи: {missing_keys}")
            except ValueError as e:
                logging.error(f"Ошибка в формате переданных данных: {e}")
                return
        try:
            with session_local.begin():
                for record in records_to_insert:
                    existing_record = session_local.query(TableNbrs).filter_by(date=record['date'],
                                                                               code=record['code']).first()
                    if existing_record:
                        logging.warning(
                            f"Запись с датой {record['date']} и кодом {record['code']} уже существует. Пропуск вставки.")
                    else:
                        session_local.execute(
                            insert(TableNbrs.__table__),
                            record
                        )
                        logging.info(f"Данные для {record['date']} и {record['code']} успешно вставлены.")

            logging.info("Вставка данных в базу данных завершена успешно.")

        except Exception as e:
            logging.error(f"Произошла ошибка при вставке записей в базу данных: {e}")
            session_local.rollback()

        finally:
            session_local.close()