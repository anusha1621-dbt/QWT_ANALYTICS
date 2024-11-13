{{ config(materialized = 'table', schema='transform', transient= false,
          pre_hook = "use warehouse dbt_queries;",
          post_hook = "create table transform.trf_customers_hook_copy clone {{this}};"
) }}
 
select 
CustomerID	,
CompanyName	,
ContactName	,
City	,
Country	,
--DivisionID	,
d.divisionname as DivisionName,
Address	,
Fax	,
Phone	,
PostalCode	,
IFF(StateProvince='', 'NA', StateProvince) AS StateProvince
from
{{ ref('stg_customers')}}  c inner join  {{ ref('lkp_division')}} as d
on c.divisionid=d.divisionid