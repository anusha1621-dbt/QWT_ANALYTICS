{{ config(materialized = 'table', schema='transform') }}

select 
GET(XMLGET(SUPPLIERSINFO, 'SupplierID'), '$') as supplierid,
GET(XMLGET(SUPPLIERSINFO, 'CompanyName'), '$')::varchar as CompanyName,
GET(XMLGET(SUPPLIERSINFO, 'ContactName'), '$')::varchar as ContactName,
GET(XMLGET(SUPPLIERSINFO, 'Address'), '$')::varchar as Address,
GET(XMLGET(SUPPLIERSINFO, 'City'), '$')::varchar as City,
GET(XMLGET(SUPPLIERSINFO, 'PostalCode'), '$')::varchar as PostalCode,
GET(XMLGET(SUPPLIERSINFO, 'Country'), '$')::varchar as Country,
GET(XMLGET(SUPPLIERSINFO, 'Phone'), '$')::varchar as Phone,
GET(XMLGET(SUPPLIERSINFO, 'Fax'), '$')::varchar as Fax

from 
{{ ref('stg_suppliers')}}