{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('nominal_codes_financ_years_period_balances_ab1') }}
select
    cast(nominal_code_id as {{ dbt_utils.type_float() }}) as nominal_code_id, 
    cast(period_balance as {{ dbt_utils.type_float() }}) as period_balance,
    cast(adjustment_after_year_end_close as {{ dbt_utils.type_float() }}) as adjustment_after_year_end_close,
    cast(consolidated_amount as {{ dbt_utils.type_float() }}) as consolidated_amount,
    cast(original_budget_value as {{ dbt_utils.type_float() }}) as original_budget_value,
    cast(financial_years_id as {{ dbt_utils.type_bigint() }}) as financial_years_id,
    cast(budget_value as {{ dbt_utils.type_float() }}) as budget_value,
    {{ cast_to_boolean('is_future_period') }} as is_future_period,
    cast(date_time_updated as timestamp) as date_time_updated,
    cast(accounting_period_id as {{ dbt_utils.type_float() }}) as accounting_period_id,
    cast(period_balance_id as {{ dbt_utils.type_float() }}) as period_balance_id,
    cast(date_time_created as timestamp) as date_time_created,
    cast(period_number as {{ dbt_utils.type_float() }}) as period_number,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nominal_codes_financ_years_period_balances_ab1') }}
-- period_balances at Nominal Codes/financial_years/period_balances
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

