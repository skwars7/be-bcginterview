import logging
from src.utils.connection import Connection
from src.utils.config import Config
import boto3

class RDSHelper:
    def __init__(self):
        self.connection = Connection.getInstance()
        if not self.is_connection_alive():
            raise Exception("DB Connectivity Issue")
        self.client = boto3.client('rds',region_name=Config.aws_region)
    
    def is_connection_alive(self):
        retry_count = 0
        is_alive =False
        while retry_count < 3:
            try:
                cursor = self.connection.cursor()
                cursor.execute("select 1;", None)
                response = cursor.fetchall()
                logging.info("Connection is alive")
                is_alive = True
                break
            except Exception as ex:
                logging.exception(ex)
                retry_count +=1
                logging.info(f"__reset_connection with retry_count : {retry_count}")
                Connection.delete_instance()
                self.connection = Connection.getInstance()
        logging.info(f"is_connection_alive : {is_alive}")
        return is_alive

    def get_result_set(
            self, rds_response, column_metadata, log_response=True
        ):
        """
        Return a list of row objects with parameter-value pairs extracted from RDS response.
        """
        column_names_list = [column[0] for column in column_metadata]
        result_set = []
        for row in rds_response:
            # Create row objects by mapping column names to row values
            result_set.append(dict(zip(column_names_list, row)))
        if log_response:
            logging.info(f'Query output converted to dict is: {result_set}')
        return result_set

    def execute_statement(
            self, sql, sql_parameters=None, cursor=None, log_response=True
        ):
        if sql_parameters is None:
            sql_parameters = {}
        logging.info(f'SQL parameters: {sql_parameters}')
        handle_transaction = False if cursor is None else True
        logging.info(f'handle transaction is: {handle_transaction}')
        if not cursor:
            cursor = self.connection.cursor()

        if handle_transaction:
            cursor.execute(sql, sql_parameters)
            response = cursor.fetchall()
            column_metadata = cursor.description
            return self.get_result_set(response, column_metadata, log_response=log_response)
        else:
            try:
                cursor.execute(sql, sql_parameters)
                response = cursor.fetchall()
                column_metadata = cursor.description
                self.connection.commit()
                cursor.close()
                return self.get_result_set(response, column_metadata, log_response=log_response)
            except Exception as error:
                self.connection.rollback()
                cursor.close()
                logging.error(f'Error happened while executing the query: {error}')
                raise error

    def execute_command(
            self, sql, sql_parameters=None, cursor=None
        ):
        if sql_parameters is None:
            sql_parameters = {}
        logging.info(f'SQL parameters: {sql_parameters}')
        handle_transaction = False if cursor is None else True
        logging.info(f'handle transaction is: {handle_transaction}')
        if not cursor:
            cursor = self.connection.cursor()

        if handle_transaction:
            cursor.execute(sql, sql_parameters)
            return cursor.rowcount
        else:
            try:
                cursor.execute(sql, sql_parameters)
                count = cursor.rowcount
                self.connection.commit()
                cursor.close()
                return count
            except Exception as error:
                self.connection.rollback()
                cursor.close()
                logging.error(f'Error happened while executing the query: {error}')
                raise error