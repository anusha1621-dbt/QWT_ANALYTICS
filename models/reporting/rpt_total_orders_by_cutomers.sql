{{config(materialized='view', schema='reporting')}}


select companyname, contactname ,city,  
dmin.DAY_OF_WEEK_NAME first_order_weekname, dmax.DAY_OF_WEEK_NAME recent_order_weekname
, total_orders, total_sales
from  (
select c.companyname, c.contactname ,c.city, 
sum(quantity) total_orders, sum(linesalesamount) total_sales, min(orderdate) min_orderdate,  
 max(orderdate) max_orderdate
from {{ref('dim_customers')}} c
inner join {{ref('fct_orders')}} f on c.customerid=f.customerid
group by  COMPANYNAME, CONTACTNAME,CITY
) m  inner join {{ref('dim_date')}}  dmin on m.MIN_ORDERDATE= dmin.DATE_DAY 
  inner join {{ref('dim_date')}}  dmax on m.MIN_ORDERDATE= dmax.DATE_DAY 
  order by total_orders desc
