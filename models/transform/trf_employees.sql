{{ config(materialized = 'table', schema='transform') }}
 
select 
e.EMPID,
e.FIRSTNAME,
e.Title,
o.ADDRESS,
o.CITY,
o.STATEPROVINCE,
o.PHONE,
e.HireDate,
iff(e2.FIRSTNAME is null,  e.FIRSTNAME ,e2.FIRSTNAME) as manager_name,
iff(e2.title is null, e.title, e2.title) as manager_title,
iff(e.Extension='-', 'NA', e.Extension) as Extension,
e.YearSalary
from {{ ref('stg_employees')}}  e inner join  {{ ref('stg_offices')}} as o on e.OFFICE=o.officeid
inner join  {{ ref('stg_employees')}} as e2 on e.REPORTSTO=e2.EMPID