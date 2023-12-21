{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sage200_etl_pte', '_airbyte_raw_currency') }}
select
    {{ json_extract_scalar('_airbyte_data', ['use_for_new_accounts'], ['use_for_new_accounts']) }} as use_for_new_accounts,
    {{ json_extract_scalar('_airbyte_data', ['symbol'], ['symbol']) }} as symbol,
    {{ json_extract_scalar('_airbyte_data', ['date_time_updated'], ['date_time_updated']) }} as date_time_updated,
    {{ json_extract_scalar('_airbyte_data', ['currency_iso_code_id'], ['currency_iso_code_id']) }} as currency_iso_code_id,
    {{ json_extract_scalar('_airbyte_data', ['exchange_rate_amendability_type'], ['exchange_rate_amendability_type']) }} as exchange_rate_amendability_type,
    {{ json_extract_scalar('_airbyte_data', ['euro_currency_rate'], ['euro_currency_rate']) }} as euro_currency_rate,
    {{ json_extract_scalar('_airbyte_data', ['core_currency_rate'], ['core_currency_rate']) }} as core_currency_rate,
    {{ json_extract_scalar('_airbyte_data', ['exchange_rate_type'], ['exchange_rate_type']) }} as exchange_rate_type,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['is_base_currency'], ['is_base_currency']) }} as is_base_currency,
    {{ json_extract_scalar('_airbyte_data', ['is_euro_currency'], ['is_euro_currency']) }} as is_euro_currency,
    {{ json_extract_scalar('_airbyte_data', ['date_time_created'], ['date_time_created']) }} as date_time_created,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sage200_etl_pte', '_airbyte_raw_currency') }} as table_alias
-- currency
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

