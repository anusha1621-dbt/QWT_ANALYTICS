{{ config ( materialized = 'table' ,  schema =env_var('DBT_STAGESCHEMA','STAGING')) }}

select * from 
{{source('my_raw_config','suppliers')}}