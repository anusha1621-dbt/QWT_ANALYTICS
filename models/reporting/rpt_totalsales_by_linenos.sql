{{config(materialized='view', schema='reporting')}}


{% set linenos =get_linenumbers() %}

select 
orderid,
{% for linenumber in linenos %}
sum(case when lineno={{linenumber}} then Linesalesamount end) as lineno{{linenumber}}_sales,
{% endfor %}
sum(Linesalesamount) as total_sales,
avg(margin) as avg_margin
from {{ref('fct_orders')}}
group by orderid
