{% snapshot shipments_snapshot %}

{{ config 

(
    target_database = 'QWT_ANALYTICS',
    target_schema = 'SNAPSHOTS',
    unique_key = "orderid||'-'||lineno",

    strategy = 'timestamp',
    updated_at = 'shipmentdate',
)
}}

select * from {{source('my_raw_config', 'shipments')}}

{% endsnapshot %}