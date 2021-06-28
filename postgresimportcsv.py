# By Drozdov S. I.

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import logger
from sql_query import *
from cred import *

FILENAME = 'LTV.csv'


class DBMSCreateConnection:

    def __init__(self, connection_string):
        """ Инициируем подключение к базе """

        self.connection_str = connection_string
        self.session = None

    def __enter__(self):
        engine = create_engine(self.connection_str)
        session = sessionmaker()
        self.session = session(bind=engine)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()


class PostgresImportCSV:

    def __init__(self):
        """ Запускаем ETL процедуры """

        self.log = logger.get_logger(self.__class__.__name__)
        self.create_table()
        self.create_sections()
        self.load_data()

    def create_table(self):
        """ Создаем таблицу car_insurance """

        commands = (
            SQL_DROP_MAIN_PARTITION_TABLE_IF_EXISTS,
            SQL_CREATE_MAIN_PARTITION_TABLE_IF_NOT_EXISTS,
            SQL_CREATE_INDEX_ON_MAIN_PARTITION_TABLE
        )

        with DBMSCreateConnection(PG_CONN) as conn:
            for command in commands:
                try:
                    conn.session.execute(command)
                except (Exception, psycopg2.DatabaseError) as error:
                    self.log.exception(error)
                finally:
                    conn.session.commit()

        self.log.info('Таблица car_insurance успешно создана')

    @staticmethod
    def get_true_data():
        """ Читаем CSV, переименовывем колонки """

        header_table_names = ['customer', 'state', 'customer_lifetime_value',
                              'response', 'coverage', 'education',
                              'effective_to_date', 'employment_status',
                              'gender', 'income', 'location_code',
                              'martial_status', 'monthly_premium_auto',
                              'months_since_last_claim',
                              'months_since_policy_inception',
                              'number_of_open_complaints', 'number_of_policies',
                              'policy_type', 'policy', 'renew_offer_type',
                              'sales_channel', 'total_claim_amount', 'vehicle_class',
                              'vehicle_size']

        try:
            data = pd.read_csv(FILENAME, delimiter=',')
            true_data = data.rename(columns=dict(zip(data, header_table_names)))
        except Exception as error:
            raise error

        return true_data

    def get_states(self):
        """ Берем уникальные значения колонки 'state' """
        # Для последующего создания секций БД и загрузки данных

        true_data = self.get_true_data()
        try:
            dd = true_data.drop_duplicates(['state'])
            states = sorted([value for value in dd['state']])
        except Exception as error:
            raise error

        return states

    def create_sections(self):
        """ Создаем партиции таблицы car_insurance """

        states = self.get_states()
        for state in states:
            commands = (
                SQL_DROP_PARTITION_TABLE_IF_EXISTS.format(state_lower=state.lower()),
                SQL_CREATE_PARTITION_TABLE.format(state_lower=state.lower(), state=state)
            )

            with DBMSCreateConnection(PG_CONN) as conn:
                for command in commands:
                    try:
                        conn.session.execute(command)
                    except (Exception, psycopg2.DatabaseError) as error:
                        self.log.exception(error)
                    finally:
                        conn.session.commit()

            self.log.info(f"Партиция {state.title()} добавлена")

    def load_data(self):
        """ Загружаем данные из csv по партициям """

        data = self.get_true_data()
        states = self.get_states()

        with DBMSCreateConnection(PG_CONN) as conn:
            for state in states:
                partition_data = data.loc[data['state'] == state]
                try:
                    partition_data.to_sql(f'car_insurance_{state.lower()}',
                                          con=conn.connection_str,
                                          if_exists='append',
                                          index=False)
                except (Exception, psycopg2.DatabaseError, ValueError) as error:
                    self.log.exception(error)
                finally:
                    conn.session.commit()


if __name__ == '__main__':
    csv_loader = PostgresImportCSV()
