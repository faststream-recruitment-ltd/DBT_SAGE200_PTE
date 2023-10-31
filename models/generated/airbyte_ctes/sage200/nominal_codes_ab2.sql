{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('nominal_codes_ab1') }}
select
    cast(department_code as {{ dbt_utils.type_string() }}) as department_code,
    cast(cost_centre_code as {{ dbt_utils.type_string() }}) as cost_centre_code,
    financial_years,
    cast(analysis_code_20 as {{ dbt_utils.type_string() }}) as analysis_code_20,
    cast(nominal_account_type as {{ dbt_utils.type_string() }}) as nominal_account_type,
    cast(reference as {{ dbt_utils.type_string() }}) as reference,
    cast(brought_forward_balance as {{ dbt_utils.type_float() }}) as brought_forward_balance,
    cast(sofa_category_id as {{ dbt_utils.type_float() }}) as sofa_category_id,
    cast(report_category_id as {{ dbt_utils.type_float() }}) as report_category_id,
    cast(consolidated_nominal_department as {{ dbt_utils.type_string() }}) as consolidated_nominal_department,
    {{ cast_to_boolean('use_batch_postings') }} as use_batch_postings,
    cast(nominal_code_id as {{ dbt_utils.type_float() }}) as nominal_code_id,   
    cast(consolidated_nominal_cost_centre as {{ dbt_utils.type_string() }}) as consolidated_nominal_cost_centre,
    cast(debit_balance_year_to_date as {{ dbt_utils.type_float() }}) as debit_balance_year_to_date,
    cast(analysis_code_2 as {{ dbt_utils.type_string() }}) as analysis_code_2,
    cast(analysis_code_1 as {{ dbt_utils.type_string() }}) as analysis_code_1,
    cast(analysis_code_4 as {{ dbt_utils.type_string() }}) as analysis_code_4,
    cast(department_id as {{ dbt_utils.type_float() }}) as department_id,
    cast(analysis_code_3 as {{ dbt_utils.type_string() }}) as analysis_code_3,
    cast(analysis_code_6 as {{ dbt_utils.type_string() }}) as analysis_code_6,
    cast(analysis_code_5 as {{ dbt_utils.type_string() }}) as analysis_code_5,
    cast(periods_to_keep_transactions as {{ dbt_utils.type_float() }}) as periods_to_keep_transactions,
    cast(analysis_code_8 as {{ dbt_utils.type_string() }}) as analysis_code_8,
    cast(analysis_code_7 as {{ dbt_utils.type_string() }}) as analysis_code_7,
    cast(date_time_updated as timestamp) as date_time_updated,
    cast(cost_centre_id as {{ dbt_utils.type_float() }}) as cost_centre_id,
    cast(analysis_code_9 as {{ dbt_utils.type_string() }}) as analysis_code_9,
    cast(analysis_code_10 as {{ dbt_utils.type_string() }}) as analysis_code_10,
    cast(analysis_code_11 as {{ dbt_utils.type_string() }}) as analysis_code_11,
    cast(analysis_code_12 as {{ dbt_utils.type_string() }}) as analysis_code_12,
    cast(analysis_code_13 as {{ dbt_utils.type_string() }}) as analysis_code_13,
    {{ cast_to_boolean('allow_manual_journal_entries') }} as allow_manual_journal_entries,
    cast(analysis_code_14 as {{ dbt_utils.type_string() }}) as analysis_code_14,
    cast(analysis_code_15 as {{ dbt_utils.type_string() }}) as analysis_code_15,
    cast(analysis_code_16 as {{ dbt_utils.type_string() }}) as analysis_code_16,
    cast(analysis_code_17 as {{ dbt_utils.type_string() }}) as analysis_code_17,
    cast(analysis_code_18 as {{ dbt_utils.type_string() }}) as analysis_code_18,
    cast(analysis_code_19 as {{ dbt_utils.type_string() }}) as analysis_code_19,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    cast(balance_year_to_date as {{ dbt_utils.type_float() }}) as balance_year_to_date,
    cast(date_time_created as timestamp) as date_time_created,
    {{ cast_to_boolean('display_balances_in_selection_list') }} as display_balances_in_selection_list,
    cast(account_status_type as {{ dbt_utils.type_string() }}) as account_status_type,
    cast(credit_balance_year_to_date as {{ dbt_utils.type_float() }}) as credit_balance_year_to_date,
    cast(consolidated_nominal_account_number as {{ dbt_utils.type_string() }}) as consolidated_nominal_account_number,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('nominal_codes_ab1') }}
-- nominal_codes
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

