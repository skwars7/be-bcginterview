#builtin-import
import json,logging,datetime

#custom import
from src.db_helper.get_dbhelper import GetDBHelper

class GetController():
    def __init__(self, event) -> None:
        self.db_helper = GetDBHelper()
        if 'pathParameters' in event and event['pathParameters'] != None:
            if 'policy_id' in event['pathParameters']:
                self.policy_id = event['pathParameters']['policy_id']
        else:
            self.policy_id = None
    
    def execute(self):
            try:
                response_obj={}
                result={}
                response_obj['statusCode'] = 200
                response_obj['headers'] = { "Content-Type": "application/json; charset=utf-8","Access-Control-Allow-Origin" : "*", "Access-Control-Allow-Credentials" : True,'Access-Control-Allow-Methods': 'OPTIONS,POST,GET' }
                if self.policy_id:
                    result['policy'] = self.db_helper.get_policy(self.policy_id)
                else:
                    result['policy'] = self.db_helper.get_policy()
                    result['pie_chart']=self.db_helper.get_pie_chart_data()
                    result['data_mon']=self.db_helper.get_pie_chart_data_mon()
                    result['area_chart']=self.db_helper.get_area_chart_data()
                    result['bar_chart']=self.db_helper.get_bar_chart_data()
                logging.info(result)
                response_obj['body'] = json.dumps(result,default=str)
                return response_obj
            except Exception as error:
                logging.error(f'{error} get error occured')
                logging.debug(error)
                raise error