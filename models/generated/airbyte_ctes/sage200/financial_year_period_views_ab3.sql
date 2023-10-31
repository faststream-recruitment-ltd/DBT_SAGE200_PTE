{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('financial_year_period_views_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'period_end_date',
        'number_of_periods_in_year',
        'period_start_date',
        'accounting_period_id',
        'year_relative_to_current_year',
        'period_number',
        'financial_year_id',
        'financial_year_start_date',
        'financial_year_end_date',
    ]) }} as _airbyte_financial_year_period_views_hashid,
    tmp.*
from {{ ref('financial_year_period_views_ab2') }} tmp
-- financial_year_period_views
where 1 = 1

