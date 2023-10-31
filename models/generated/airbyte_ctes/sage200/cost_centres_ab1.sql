{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sage200_etl_pte', '_airbyte_raw_cost_centres') }}
select
    {{ json_extract_scalar('_airbyte_data', ['contact_name'], ['contact_name']) }} as contact_name,
    {{ json_extract_scalar('_airbyte_data', ['code'], ['code']) }} as code,
    {{ json_extract_scalar('_airbyte_data', ['contact_email_address'], ['contact_email_address']) }} as contact_email_address,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['date_time_updated'], ['date_time_updated']) }} as date_time_updated,
    {{ json_extract_scalar('_airbyte_data', ['contact_details'], ['contact_details']) }} as contact_details,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['date_time_created'], ['date_time_created']) }} as date_time_created,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sage200_etl_pte', '_airbyte_raw_cost_centres') }} as table_alias
-- cost_centres
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

