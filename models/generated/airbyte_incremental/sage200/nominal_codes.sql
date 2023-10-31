{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "sage200_etl_pte",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('nominal_codes_scd') }}
select
    _airbyte_unique_key,
    department_code,
    cost_centre_code,
    financial_years,
    analysis_code_20,
    nominal_account_type,
    reference,
    brought_forward_balance,
    sofa_category_id,
    report_category_id,
    consolidated_nominal_department,
    use_batch_postings,
    nominal_code_id,
    consolidated_nominal_cost_centre,
    debit_balance_year_to_date,
    analysis_code_2,
    analysis_code_1,
    analysis_code_4,
    department_id,
    analysis_code_3,
    analysis_code_6,
    analysis_code_5,
    periods_to_keep_transactions,
    analysis_code_8,
    analysis_code_7,
    date_time_updated,
    cost_centre_id,
    analysis_code_9,
    analysis_code_10,
    analysis_code_11,
    analysis_code_12,
    analysis_code_13,
    allow_manual_journal_entries,
    analysis_code_14,
    analysis_code_15,
    analysis_code_16,
    analysis_code_17,
    analysis_code_18,
    analysis_code_19,
    {{ adapter.quote('name') }},
    balance_year_to_date,
    date_time_created,
    display_balances_in_selection_list,
    account_status_type,
    credit_balance_year_to_date,
    consolidated_nominal_account_number,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_nominal_codes_hashid
from {{ ref('nominal_codes_scd') }}
-- nominal_codes from {{ source('sage200_etl_pte', '_airbyte_raw_nominal_codes') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

