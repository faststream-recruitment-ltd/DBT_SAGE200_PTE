{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('departments_ab1') }}
select
    cast(contact_name as {{ dbt_utils.type_string() }}) as contact_name,
    cast(code as {{ dbt_utils.type_string() }}) as code,
    cast(contact_email_address as {{ dbt_utils.type_string() }}) as contact_email_address,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    cast(date_time_updated as timestamp) as date_time_updated,
    cast(contact_details as {{ dbt_utils.type_string() }}) as contact_details,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('id') }},
    cast(date_time_created as timestamp) as date_time_created,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('departments_ab1') }}
-- departments
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

