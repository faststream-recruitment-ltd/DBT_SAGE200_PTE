{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "sage200_etl_pte",
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('nominal_codes_financial_years_scd') }}
select
    budget_value,
    nominal_code_id,
    date_time_updated,
    period_balances,
    adjustment_after_year_end_close,
    original_budget_value,
    budget_profile_id,
    closing_balance,
    financial_years_id,
    date_time_created,
    year_relative_to_current_year,
    financial_year_id,
    budget_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_financial_years_hashid
from {{ ref('nominal_codes_financial_years_scd') }}
-- financial_years from {{ source('sage200_etl_pte', '_airbyte_raw_nominal_codes') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

