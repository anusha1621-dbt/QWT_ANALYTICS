{{ config ( materialized = 'table', schema =env_var('DBT_STAGESCHEMA','STAGING')) }}
 
 
select 
    EmpID,
    LastName,
    FIRSTNAME,
    Title,
    to_date(HireDate, 'MM/DD/YY') as HireDate,
    Office,
    EXTENSION,
    ReportsTo,
    YearSalary
 from
{{ source('my_raw_config','employees' )}}