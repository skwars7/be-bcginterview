from urllib import response
from src.utils.rds_helper import RDSHelper

class GetDBHelper(RDSHelper):
    def get_policy(self,policy_id=None):
        query = "select policy_id,date_of_purchase,customer_id,fuel,vehicle_segment,premium,case when bodily_injury_liability = true then 'True' else 'False' end as bodily_injury_liability , case when personal_injury_protection = true then 'True' else 'False' end as personal_injury_protection ,case when property_damage_liability = true then 'True' else 'False' end as property_damage_liability,case when collision = true then 'True' else 'False' end as collision,case when comprehensive = true then 'True' else 'False' end as comprehensive,customer_gender,customer_income_group,customer_region,case when customer_marital_status = true then 'True' else 'False' end as customer_marital_status from policy"
        if policy_id:
            query += " where policy_id in %(policy_id)s"
            sql_param = {'policy_id':(policy_id,)}
        else:
            sql_param = None
        response = self.execute_statement(query,sql_param,log_response=False)
        return response

    def get_pie_chart_data(self):
        response = self.execute_statement("select sum(premium) as premium, customer_region from policy group by customer_region ",log_response=False)
        return response

    def get_pie_chart_data_mon(self):
        response = self.execute_statement("select sum(premium) as premium, extract( month FROM date_of_purchase::DATE) as month_of_purchase from policy group by extract( month FROM date_of_purchase::DATE) ",log_response=False)
        return response
    
    def get_area_chart_data(self):
        response = self.execute_statement("select sum(premium) as premium, date_of_purchase from policy group by date_of_purchase ",log_response=False)
        return response

    def get_bar_chart_data(self):
        response = self.execute_statement("select sum(premium) as premium, customer_income_group from policy group by customer_income_group ",log_response=False)
        return response