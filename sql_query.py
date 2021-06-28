SQL_DROP_MAIN_PARTITION_TABLE_IF_EXISTS = """
DROP TABLE IF EXISTS public.car_insurance
"""

SQL_CREATE_MAIN_PARTITION_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS public.car_insurance (
    customer TEXT NOT NULL,
    state TEXT NOT NULL,
    customer_lifetime_value INT NOT NULL,
    response TEXT NOT NULL,
    coverage TEXT NOT NULL,
    education TEXT NOT NULL,
    effective_to_date DATE NOT NULL,
    employment_status TEXT NOT NULL,
    gender TEXT NOT NULL,
    income INT NOT NULL,
    location_code TEXT NOT NULL,
    martial_status TEXT NOT NULL,
    monthly_premium_auto INT NOT NULL,
    months_since_last_claim INT NOT NULL,
    months_since_policy_inception INT NOT NULL,
    number_of_open_complaints INT NOT NULL,
    number_of_policies INT NOT NULL,
    policy_type TEXT NOT NULL,
    policy TEXT NOT NULL,
    renew_offer_type TEXT NOT NULL,
    sales_channel TEXT NOT NULL,
    total_claim_amount INT NOT NULL,
    vehicle_class TEXT NOT NULL,
    vehicle_size TEXT NOT NULL,
    PRIMARY KEY(customer, state, effective_to_date)
) PARTITION BY list (state)
"""

SQL_CREATE_INDEX_ON_MAIN_PARTITION_TABLE = """
CREATE INDEX ON public.car_insurance (state)
"""
SQL_DROP_PARTITION_TABLE_IF_EXISTS = """
DROP TABLE IF EXISTS public.car_insurance_{state_lower}
"""

SQL_CREATE_PARTITION_TABLE = """
CREATE TABLE car_insurance_{state_lower} 
PARTITION OF public.car_insurance 
FOR VALUES IN ('{state}')
"""
