{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('nominal_codes_financ_years_period_balances_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'nominal_code_id',
        'period_balance',
        'adjustment_after_year_end_close',
        'consolidated_amount',
        'original_budget_value',
        'financial_years_id',
        'budget_value',
        boolean_to_string('is_future_period'),
        'date_time_updated',
        'accounting_period_id',
        'period_balance_id',
        'date_time_created',
        'period_number',
    ]) }} as _airbyte_period_balances_hashid,
    tmp.*
from {{ ref('nominal_codes_financ_years_period_balances_ab2') }} tmp
-- period_balances at Nominal Codes/financial_years/period_balances
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

