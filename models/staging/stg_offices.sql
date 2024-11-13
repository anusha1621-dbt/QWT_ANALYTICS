{{ config ( materialized = 'table') }}
 
 
select  
office as officeid,
officeaddress as address,
OFFICEPOSTACLCODE as postalcode,
officecity as city,
OFFICESTATEPROVISION as stateprovince,
officephone as phone,
officefax as fax,
officecountry as country from
QWT_ANALYTICS.SCH_RAW.OFFICES