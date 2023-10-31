{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('nominal_codes_financial_years_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'budget_value',
        'nominal_code_id',
        'date_time_updated',
        array_to_string('period_balances'),
        'adjustment_after_year_end_close',
        'original_budget_value',
        'budget_profile_id',
        'closing_balance',
        'financial_years_id',
        'date_time_created',
        'year_relative_to_current_year',
        'financial_year_id',
        'budget_type',
    ]) }} as _airbyte_financial_years_hashid,
    tmp.*
from {{ ref('nominal_codes_financial_years_ab2') }} tmp
-- financial_years at Nominal Codes/financial_years
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

