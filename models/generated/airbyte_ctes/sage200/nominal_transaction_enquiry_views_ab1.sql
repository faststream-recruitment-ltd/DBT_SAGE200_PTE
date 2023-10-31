{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sage200_etl_pte', '_airbyte_raw_nominal_transaction_enquiry_views') }}
select
    {{ json_extract_scalar('_airbyte_data', ['transaction_date'], ['transaction_date']) }} as transaction_date,
    {{ json_extract_scalar('_airbyte_data', ['transaction_analysis_code'], ['transaction_analysis_code']) }} as transaction_analysis_code,
    {{ json_extract_scalar('_airbyte_data', ['user_name'], ['user_name']) }} as user_name,
    {{ json_extract_scalar('_airbyte_data', ['narrative'], ['narrative']) }} as narrative,
    {{ json_extract_scalar('_airbyte_data', ['nominal_code_id'], ['nominal_code_id']) }} as nominal_code_id,
    {{ json_extract_scalar('_airbyte_data', ['source'], ['source']) }} as {{ adapter.quote('source') }},
    {{ json_extract_scalar('_airbyte_data', ['period_number'], ['period_number']) }} as period_number,
    {{ json_extract_scalar('_airbyte_data', ['urn'], ['urn']) }} as urn,
    {{ json_extract_scalar('_airbyte_data', ['reference'], ['reference']) }} as reference,
    {{ json_extract_scalar('_airbyte_data', ['accounting_period_id'], ['accounting_period_id']) }} as accounting_period_id,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['debit'], ['debit']) }} as debit,
    {{ json_extract_scalar('_airbyte_data', ['credit'], ['credit']) }} as credit,
    {{ json_extract_scalar('_airbyte_data', ['year_relative_to_current_year'], ['year_relative_to_current_year']) }} as year_relative_to_current_year,
    {{ json_extract_scalar('_airbyte_data', ['financial_year_id'], ['financial_year_id']) }} as financial_year_id,
    {{ json_extract_scalar('_airbyte_data', ['financial_year_start_date'], ['financial_year_start_date']) }} as financial_year_start_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sage200_etl_pte', '_airbyte_raw_nominal_transaction_enquiry_views') }} as table_alias
-- nominal_transaction_enquiry_views
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

