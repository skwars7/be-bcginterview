#builtin-import
import json,logging,boto3,base64,datetime

#custom import
from src.db_helper.post_dbhelper import PostDBHelper
import src.utils.exceptions as exceptions
class PostController():
    def __init__(self, event) -> None:
        self.query_string = event['queryStringParameters']
        if self.query_string['premium'] > 1000000:
            raise exceptions.CustomException("Premium is greter then 1 mil")
        self.policy_id = self.query_string['policy_id']
        self.db_helper= PostDBHelper()

    def execute(self):
            response_obj=response={}
            response_obj['statusCode'] = 200
            response_obj['headers'] = {"Content-Type": "application/json; charset=utf-8","Access-Control-Allow-Origin" : "*", "Access-Control-Allow-Credentials" : True,'Access-Control-Allow-Methods': 'OPTIONS,POST,GET' }
            try:
                params = {
                    'policy_id': self.policy_id,
                    'date_of_purchase': self.query_string['date_of_purchase'],
                    'customer_id': self.query_string['customer_id'],
                    'fuel': self.query_string['fuel'],
                    'vehicle_segment': self.query_string['vehicle_segment'],
                    'premium': self.query_string['premium'],
                    'bodily_injury_liability': True if self.query_string['bodily_injury_liability'].lower() == 'true' else False,
                    'personal_injury_protection': True if self.query_string['personal_injury_protection'].lower() == 'true' else False,
                    'property_damage_liability': True if self.query_string['property_damage_liability'].lower() == 'true' else False,
                    'collision': True if self.query_string['collision'].lower() == 'true' else False,
                    'comprehensive': True if self.query_string['comprehensive'].lower() == 'true' else False,
                    'customer_gender': self.query_string['customer_gender'],
                    'customer_income_group': self.query_string['customer_income_group'],
                    'customer_region': self.query_string['customer_region'],
                    'customer_marital_status': True if self.query_string['customer_marital_status'].lower() == 'true' else False,
                }
                self.db_helper.update_row(params)
                return response_obj
            except Exception as erro:
                logging.info(f'{erro} post error occured')
                return {'statusCode':502,'body':[f'{erro} Please Try Again']}