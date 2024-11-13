import snowflake.snowpark.functions as F
 
def model(dbt, session):
 
    dbt.config(materialized = "table", schema = "reporting")

    dim_customers_df = dbt.ref('dim_customers')

    fct_orders_df = dbt.ref('fct_orders')

    customers_orders_df=(fct_orders_df.group_by('customerid')
                                      .agg(
                                           F.min(F.col('orderdate')).alias('first_orderdate'),
                                           F.max(F.col('orderdate')).alias('recent_orderdate'),
                                           F.sum(F.col('quantity')).alias('total_orders'),
                                           F.min(F.col('linesalesamount')).alias('total_sales')
                                         )
                        )
    final_df=(
        dim_customers_df
        .join(customers_orders_df,dim_customers_df.customerid==customers_orders_df.customerid, 'left')
        .select(
                dim_customers_df.companyname.alias('companyname'),
                dim_customers_df.contactname.alias('contactname'),
                dim_customers_df.city.alias('city'),
                customers_orders_df.first_orderdate.alias('first_order_date'),
                customers_orders_df.recent_orderdate.alias('recent_order_date'),
                customers_orders_df.total_orders.alias('total_orders'),
                customers_orders_df.total_sales.alias('total_sales')
                )  
        )
    return final_df