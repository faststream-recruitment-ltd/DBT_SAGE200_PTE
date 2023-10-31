{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('nominal_codes_financial_years') }}
{{ unnest_cte(ref('nominal_codes_financial_years'), 'financial_years', 'period_balances') }}
select
    table_alias.nominal_code_id,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['period_balance'], ['period_balance']) }} as period_balance,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['adjustment_after_year_end_close'], ['adjustment_after_year_end_close']) }} as adjustment_after_year_end_close,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['consolidated_amount'], ['consolidated_amount']) }} as consolidated_amount,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['original_budget_value'], ['original_budget_value']) }} as original_budget_value,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['nominal_account_year_value_id'], ['nominal_account_year_value_id']) }} as financial_years_id,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['budget_value'], ['budget_value']) }} as budget_value,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['is_future_period'], ['is_future_period']) }} as is_future_period,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['date_time_updated'], ['date_time_updated']) }} as date_time_updated,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['accounting_period_id'], ['accounting_period_id']) }} as accounting_period_id,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['id'], ['id']) }} as period_balance_id,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['date_time_created'], ['date_time_created']) }} as date_time_created,
    {{ json_extract_scalar(unnested_column_value('period_balances'), ['period_number'], ['period_number']) }} as period_number,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nominal_codes_financial_years') }} as table_alias
-- period_balances at Nominal Codes/financial_years/period_balances
{{ cross_join_unnest('financial_years', 'period_balances') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

