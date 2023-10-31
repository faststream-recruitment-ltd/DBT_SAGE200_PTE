{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('nominal_codes_scd') }}
{{ unnest_cte(ref('nominal_codes_scd'), 'nominal_codes', 'financial_years') }}
select
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['budget_value'], ['budget_value']) }} as budget_value,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['nominal_code_id'], ['nominal_code_id']) }} as nominal_code_id,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['date_time_updated'], ['date_time_updated']) }} as date_time_updated,
    {{ json_extract_array(unnested_column_value('financial_years'), ['period_balances'], ['period_balances']) }} as period_balances,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['adjustment_after_year_end_close'], ['adjustment_after_year_end_close']) }} as adjustment_after_year_end_close,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['original_budget_value'], ['original_budget_value']) }} as original_budget_value,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['budget_profile_id'], ['budget_profile_id']) }} as budget_profile_id,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['closing_balance'], ['closing_balance']) }} as closing_balance,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['id'], ['id']) }} as financial_years_id,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['date_time_created'], ['date_time_created']) }} as date_time_created,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['year_relative_to_current_year'], ['year_relative_to_current_year']) }} as year_relative_to_current_year,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['financial_year_id'], ['financial_year_id']) }} as financial_year_id,
    {{ json_extract_scalar(unnested_column_value('financial_years'), ['budget_type'], ['budget_type']) }} as budget_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nominal_codes_scd') }} as table_alias
-- financial_years at Nominal Codes/financial_years
{{ cross_join_unnest('nominal_codes', 'financial_years') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

