with
    sap_adw_store as(
        select
             cast(businessentityid as int64) as store_business_entity_id
            ,cast(name as string) as store_name
            ,cast(salespersonid as int64) as store_sales_person_id
            ,datetime(parse_timestamp('%Y-%m-%d %H:%M:%E*S', modifieddate)) as store_modified_date
        from {{source('adw', 'store')}}
    )

select *
from sap_adw_store