def model (dbt, session):
    dbt.config(materialized='table',  schema='staging')
    stg_customer_df=dbt.source('my_raw_config', 'customers')
    return stg_customer_df