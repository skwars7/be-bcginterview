from json.tool import main
import logging
from src.controller.get_controller import GetController
from src.controller.post_controller import PostController
from src.utils.connection  import Connection
import src.utils.exceptions as exceptions

def lambda_handler(event, context):
    logging.getLogger()
    logging.info(f'Logging Started {event}')

    try:
        if Connection.getInstance():
            source = event['httpMethod']
            controller = None
            if source == 'POST': 
                controller = PostController
            elif source == 'GET': 
                controller = GetController
            if not controller:
                logging.error(f'DBConnection Failed')
                raise exceptions.CustomException("Source not supported")
            return controller(event).execute()
        else:
            logging.error(f'DBConnection Failed')
            raise exceptions.DatabaseConnectionFailureException()
    except Exception as error:
        logging.debug(f'{error} unexcepted error Something Went wrong')
        return {
            'statusCode':200,
            'headers':{ "Content-Type": "application/json; charset=utf-8","Access-Control-Allow-Origin" : "*", "Access-Control-Allow-Credentials" : True,'Access-Control-Allow-Methods': 'OPTIONS,POST,GET' },
            'body': f' Somehting went wrong {error} '
        }