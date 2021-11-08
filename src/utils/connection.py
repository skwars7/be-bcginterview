import psycopg2
import boto3
import logging
import aiopg
from src.utils.config import Config

class Connection:
    __instance = None

    def __init__(self):
        if not Connection.__instance:
            logging.info("__init__ method called but nothing is created")
        else:
            logging.info("instance already created:", self.getInstance())
        
    @classmethod
    def getInstance(cls, config=None, statement_timeout=900, lock_timeout=900, idle_in_transaction_session_timeout=900):
        if cls.__instance is None:
            try:
                config = config or Config()
                client = boto3.client("rds",region_name=Config.aws_region)

                if config.use_serverless_db:
                    password = config.db_password
                else:
                    password = client.generate_db_auth_token(
                        DBHostname=config.host_uri, Port=config.port_num, DBUsername=config.db_user_name)

                conn = psycopg2.connect(
                    host=config.host_uri,
                    port=config.port_num,
                    user=config.db_user_name,
                    database=config.dbname,
                    password=password,
                    options=f'--statement_timeout={statement_timeout}s --lock_timeout={lock_timeout}s --idle_in_transaction_session_timeout={idle_in_transaction_session_timeout}s')
                cls.__instance = conn

            except psycopg2.Error as e:
                logging.info('Error happened while creating db connection  ' + str(e))

        return cls.__instance

    @classmethod
    def get_async_instance(cls, config=None, timeout=900):

        async def go(config=None):
            config = config or Config()
            client = boto3.client("rds")
            password = client.generate_db_auth_token(
                DBHostname=config.host_uri, Port=config.port_num, DBUsername=config.db_user_name)

            dsn = f"dbname={config.database_name} user={config.db_user_name} host={config.host_uri} " \
                  f"port={config.port_num} password={password} " \
                  "sslmode='require' sslrootcert='rds-ca-2019-root.pem'"

            conn = await aiopg.connect(dsn,timeout=timeout)
            return conn

        return go(config)

    @classmethod
    def delete_instance(cls):
        try:
            cls.__instance.close()
            logging.info("Connection closed Successfully")
        except Exception as ex:
            logging.exception(ex)
            logging.error("Failed to close connetion")
        
        cls.__instance = None
