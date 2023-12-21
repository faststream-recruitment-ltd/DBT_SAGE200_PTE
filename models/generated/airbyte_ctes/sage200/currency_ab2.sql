{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('currency_ab1') }}
select
    {{ cast_to_boolean('use_for_new_accounts') }} as use_for_new_accounts,
    cast(symbol as {{ dbt_utils.type_string() }}) as symbol,
    cast(date_time_updated as {{ dbt_utils.type_string() }}) as date_time_updated,
    cast(currency_iso_code_id as {{ dbt_utils.type_float() }}) as currency_iso_code_id,
    cast(exchange_rate_amendability_type as {{ dbt_utils.type_string() }}) as exchange_rate_amendability_type,
    cast(euro_currency_rate as {{ dbt_utils.type_float() }}) as euro_currency_rate,
    cast(core_currency_rate as {{ dbt_utils.type_float() }}) as core_currency_rate,
    cast(exchange_rate_type as {{ dbt_utils.type_string() }}) as exchange_rate_type,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('id') }},
    {{ cast_to_boolean('is_base_currency') }} as is_base_currency,
    {{ cast_to_boolean('is_euro_currency') }} as is_euro_currency,
    cast(date_time_created as {{ dbt_utils.type_string() }}) as date_time_created,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('currency_ab1') }}
-- currency
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

