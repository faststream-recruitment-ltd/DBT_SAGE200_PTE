{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('nominal_codes_financial_years_ab1') }}
select
    cast(budget_value as {{ dbt_utils.type_float() }}) as budget_value,
    cast(nominal_code_id as {{ dbt_utils.type_float() }}) as nominal_code_id, 
    cast(date_time_updated as timestamp) as date_time_updated,
    period_balances,
    cast(adjustment_after_year_end_close as {{ dbt_utils.type_float() }}) as adjustment_after_year_end_close,
    cast(original_budget_value as {{ dbt_utils.type_float() }}) as original_budget_value,
    cast(budget_profile_id as {{ dbt_utils.type_float() }}) as budget_profile_id,
    cast(closing_balance as {{ dbt_utils.type_float() }}) as closing_balance,
    cast(financial_years_id as {{ dbt_utils.type_bigint() }}) as financial_years_id,
    cast(date_time_created as timestamp) as date_time_created,
    cast(year_relative_to_current_year as {{ dbt_utils.type_float() }}) as year_relative_to_current_year,
    cast(financial_year_id as {{ dbt_utils.type_float() }}) as financial_year_id,
    cast(budget_type as {{ dbt_utils.type_string() }}) as budget_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nominal_codes_financial_years_ab1') }}
-- financial_years at Nominal Codes/financial_years
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

