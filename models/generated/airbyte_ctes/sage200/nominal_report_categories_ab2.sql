{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('nominal_report_categories_ab1') }}
select
    cast(code as {{ dbt_utils.type_string() }}) as code,
    cast(account_report_category_type as {{ dbt_utils.type_string() }}) as account_report_category_type,
    cast(account_report_type as {{ dbt_utils.type_string() }}) as account_report_type,
    cast(description as {{ dbt_utils.type_string() }}) as description,
    cast(date_time_updated as timestamp) as date_time_updated,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('id') }},
    cast(date_time_created as timestamp) as date_time_created,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nominal_report_categories_ab1') }}
-- nominal_report_categories
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

