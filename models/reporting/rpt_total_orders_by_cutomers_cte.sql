{{config(materialized='view', schema='reporting')}}


with cteSQ1 AS(
        select c.companyname, c.contactname ,c.city, 
        sum(quantity) total_orders, sum(linesalesamount) total_sales, min(orderdate) first_orderdate,  
        max(orderdate) last_orderdate
        from {{ref('dim_customers')}} c
        inner join {{ref('fct_orders')}} f on c.customerid=f.customerid
        group by  COMPANYNAME, CONTACTNAME,CITY
)


select companyname, contactname ,city,  
 first_orderdate, last_orderdate , total_orders, total_sales from  cteSQ1
  order by total_orders desc
