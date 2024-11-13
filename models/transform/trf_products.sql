{{ config(materialized = 'table', schema='transform') }}

select 
p.productid,
p.productname,
ts.CompanyName,
ts.ContactName,
ts.city,
c.categoryname,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
IFF(p.unitsonorder >p.unitsinstock, 'Product not available', 'Product is available') as productavailablity,
p.unitprice - p.unitcost as profit

from 
{{ ref('stg_products')}} p inner join {{ref( 'trf_suppliers')}} ts
 on p.supplierid=ts.supplierid
inner join {{ref( 'lkp_categories')}} c on c.categoryid=p.categoryid

