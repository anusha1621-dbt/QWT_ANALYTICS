{{ config(materialized = 'table', schema='transform') }}
 
select 
st.orderid,
st.lineno,
sr.CompanyName,
st.shipmentdate,
st.status,
st.dbt_valid_from,
st.dbt_valid_to
from
{{ ref('lkp_shippers')}}  sr inner join  {{ ref('shipments_snapshot')}} as st
on sr.shipperid=st.shipperid