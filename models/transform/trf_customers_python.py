def model (dbt, session):
    trf_customer_df=dbt.ref('stg_customers')
    return trf_customer_df