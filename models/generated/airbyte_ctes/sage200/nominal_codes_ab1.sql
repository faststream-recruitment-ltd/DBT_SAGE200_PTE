{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sage200_etl_pte', '_airbyte_raw_nominal_codes') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as nominal_code_id,
    {{ json_extract_scalar('_airbyte_data', ['department_code'], ['department_code']) }} as department_code,
    {{ json_extract_scalar('_airbyte_data', ['cost_centre_code'], ['cost_centre_code']) }} as cost_centre_code,
    {{ json_extract_array('_airbyte_data', ['financial_years'], ['financial_years']) }} as financial_years,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_20'], ['analysis_code_20']) }} as analysis_code_20,
    {{ json_extract_scalar('_airbyte_data', ['nominal_account_type'], ['nominal_account_type']) }} as nominal_account_type,
    {{ json_extract_scalar('_airbyte_data', ['reference'], ['reference']) }} as reference,
    {{ json_extract_scalar('_airbyte_data', ['brought_forward_balance'], ['brought_forward_balance']) }} as brought_forward_balance,
    {{ json_extract_scalar('_airbyte_data', ['sofa_category_id'], ['sofa_category_id']) }} as sofa_category_id,
    {{ json_extract_scalar('_airbyte_data', ['report_category_id'], ['report_category_id']) }} as report_category_id,
    {{ json_extract_scalar('_airbyte_data', ['consolidated_nominal_department'], ['consolidated_nominal_department']) }} as consolidated_nominal_department,
    {{ json_extract_scalar('_airbyte_data', ['use_batch_postings'], ['use_batch_postings']) }} as use_batch_postings,
    {{ json_extract_scalar('_airbyte_data', ['consolidated_nominal_cost_centre'], ['consolidated_nominal_cost_centre']) }} as consolidated_nominal_cost_centre,
    {{ json_extract_scalar('_airbyte_data', ['debit_balance_year_to_date'], ['debit_balance_year_to_date']) }} as debit_balance_year_to_date,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_2'], ['analysis_code_2']) }} as analysis_code_2,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_1'], ['analysis_code_1']) }} as analysis_code_1,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_4'], ['analysis_code_4']) }} as analysis_code_4,
    {{ json_extract_scalar('_airbyte_data', ['department_id'], ['department_id']) }} as department_id,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_3'], ['analysis_code_3']) }} as analysis_code_3,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_6'], ['analysis_code_6']) }} as analysis_code_6,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_5'], ['analysis_code_5']) }} as analysis_code_5,
    {{ json_extract_scalar('_airbyte_data', ['periods_to_keep_transactions'], ['periods_to_keep_transactions']) }} as periods_to_keep_transactions,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_8'], ['analysis_code_8']) }} as analysis_code_8,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_7'], ['analysis_code_7']) }} as analysis_code_7,
    {{ json_extract_scalar('_airbyte_data', ['date_time_updated'], ['date_time_updated']) }} as date_time_updated,
    {{ json_extract_scalar('_airbyte_data', ['cost_centre_id'], ['cost_centre_id']) }} as cost_centre_id,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_9'], ['analysis_code_9']) }} as analysis_code_9,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_10'], ['analysis_code_10']) }} as analysis_code_10,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_11'], ['analysis_code_11']) }} as analysis_code_11,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_12'], ['analysis_code_12']) }} as analysis_code_12,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_13'], ['analysis_code_13']) }} as analysis_code_13,
    {{ json_extract_scalar('_airbyte_data', ['allow_manual_journal_entries'], ['allow_manual_journal_entries']) }} as allow_manual_journal_entries,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_14'], ['analysis_code_14']) }} as analysis_code_14,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_15'], ['analysis_code_15']) }} as analysis_code_15,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_16'], ['analysis_code_16']) }} as analysis_code_16,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_17'], ['analysis_code_17']) }} as analysis_code_17,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_18'], ['analysis_code_18']) }} as analysis_code_18,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_19'], ['analysis_code_19']) }} as analysis_code_19,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['balance_year_to_date'], ['balance_year_to_date']) }} as balance_year_to_date,
    {{ json_extract_scalar('_airbyte_data', ['date_time_created'], ['date_time_created']) }} as date_time_created,
    {{ json_extract_scalar('_airbyte_data', ['display_balances_in_selection_list'], ['display_balances_in_selection_list']) }} as display_balances_in_selection_list,
    {{ json_extract_scalar('_airbyte_data', ['account_status_type'], ['account_status_type']) }} as account_status_type,
    {{ json_extract_scalar('_airbyte_data', ['credit_balance_year_to_date'], ['credit_balance_year_to_date']) }} as credit_balance_year_to_date,
    {{ json_extract_scalar('_airbyte_data', ['consolidated_nominal_account_number'], ['consolidated_nominal_account_number']) }} as consolidated_nominal_account_number,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sage200_etl_pte', '_airbyte_raw_nominal_codes') }} as table_alias
-- nominal_codes
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

