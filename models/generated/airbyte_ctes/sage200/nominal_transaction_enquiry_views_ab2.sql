{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('nominal_transaction_enquiry_views_ab1') }}
select
    cast(transaction_date as {{ dbt_utils.type_string() }}) as transaction_date,
    cast(transaction_analysis_code as {{ dbt_utils.type_string() }}) as transaction_analysis_code,
    cast(user_name as {{ dbt_utils.type_string() }}) as user_name,
    cast(narrative as {{ dbt_utils.type_string() }}) as narrative,
    cast(nominal_code_id as {{ dbt_utils.type_float() }}) as nominal_code_id,
    cast({{ adapter.quote('source') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('source') }},
    cast(period_number as {{ dbt_utils.type_float() }}) as period_number,
    cast(urn as {{ dbt_utils.type_float() }}) as urn,
    cast(reference as {{ dbt_utils.type_string() }}) as reference,
    cast(accounting_period_id as {{ dbt_utils.type_float() }}) as accounting_period_id,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('id') }},
    cast(debit as {{ dbt_utils.type_float() }}) as debit,
    cast(credit as {{ dbt_utils.type_float() }}) as credit,
    cast(year_relative_to_current_year as {{ dbt_utils.type_float() }}) as year_relative_to_current_year,
    cast(financial_year_id as {{ dbt_utils.type_float() }}) as financial_year_id,
    cast(financial_year_start_date as {{ dbt_utils.type_string() }}) as financial_year_start_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nominal_transaction_enquiry_views_ab1') }}
-- nominal_transaction_enquiry_views
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

