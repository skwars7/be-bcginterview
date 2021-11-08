from src.utils.rds_helper import RDSHelper

class PostDBHelper(RDSHelper):
    def update_row(self,params):
        response = self.execute_command("update policy set fuel = %(fuel)s, vehicle_segment = %(vehicle_segment)s, premium = %(premium)s, bodily_injury_liability = %(bodily_injury_liability)s, personal_injury_protection = %(personal_injury_protection)s, property_damage_liability = %(property_damage_liability)s, collision = %(collision)s, comprehensive = %(comprehensive)s, customer_gender = %(customer_gender)s, customer_income_group = %(customer_income_group)s, customer_region = %(customer_region)s, customer_marital_status = %(customer_marital_status)s where policy_id = %(policy_id)s",params)
        return response