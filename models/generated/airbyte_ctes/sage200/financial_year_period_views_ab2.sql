{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('financial_year_period_views_ab1') }}
select
    cast(period_end_date as timestamp) as period_end_date,
    cast(number_of_periods_in_year as {{ dbt_utils.type_float() }}) as number_of_periods_in_year,
    cast(period_start_date as timestamp) as period_start_date,
    cast(accounting_period_id as {{ dbt_utils.type_float() }}) as accounting_period_id,
    cast(year_relative_to_current_year as {{ dbt_utils.type_float() }}) as year_relative_to_current_year,
    cast(period_number as {{ dbt_utils.type_float() }}) as period_number,
    cast(financial_year_id as {{ dbt_utils.type_float() }}) as financial_year_id,
    cast(financial_year_start_date as timestamp) as financial_year_start_date,
    cast(financial_year_end_date as timestamp) as financial_year_end_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('financial_year_period_views_ab1') }}
-- financial_year_period_views
where 1 = 1

