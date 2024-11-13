{{config(materialized='view', schema='reporting')}}

select 
c.companyname, c.contactname , sum(linesalesamount) linesalesamount,
 sum(quantity) total_orders ,avg(margin) avg_margin 
 from  {{ref('dim_customers')}}  c
inner join   {{ref('fct_orders')}}  o on c.customerid=o.customerid
where city ={{ var('v_city', 'London')}}
group by companyname,contactname  
order by linesalesamount desc
