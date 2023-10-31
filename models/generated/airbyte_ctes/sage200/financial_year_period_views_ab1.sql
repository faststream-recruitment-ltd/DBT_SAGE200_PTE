{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sage200_etl_pte', '_airbyte_raw_financial_year_period_views') }}
select
    {{ json_extract_scalar('_airbyte_data', ['period_end_date'], ['period_end_date']) }} as period_end_date,
    {{ json_extract_scalar('_airbyte_data', ['number_of_periods_in_year'], ['number_of_periods_in_year']) }} as number_of_periods_in_year,
    {{ json_extract_scalar('_airbyte_data', ['period_start_date'], ['period_start_date']) }} as period_start_date,
    {{ json_extract_scalar('_airbyte_data', ['accounting_period_id'], ['accounting_period_id']) }} as accounting_period_id,
    {{ json_extract_scalar('_airbyte_data', ['year_relative_to_current_year'], ['year_relative_to_current_year']) }} as year_relative_to_current_year,
    {{ json_extract_scalar('_airbyte_data', ['period_number'], ['period_number']) }} as period_number,
    {{ json_extract_scalar('_airbyte_data', ['financial_year_id'], ['financial_year_id']) }} as financial_year_id,
    {{ json_extract_scalar('_airbyte_data', ['financial_year_start_date'], ['financial_year_start_date']) }} as financial_year_start_date,
    {{ json_extract_scalar('_airbyte_data', ['financial_year_end_date'], ['financial_year_end_date']) }} as financial_year_end_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sage200_etl_pte', '_airbyte_raw_financial_year_period_views') }} as table_alias
-- financial_year_period_views
where 1 = 1

