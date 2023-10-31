{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('nominal_codes_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'department_code',
        'cost_centre_code',
        array_to_string('financial_years'),
        'analysis_code_20',
        'nominal_account_type',
        'reference',
        'brought_forward_balance',
        'sofa_category_id',
        'report_category_id',
        'consolidated_nominal_department',
        boolean_to_string('use_batch_postings'),
        'nominal_code_id',
        'consolidated_nominal_cost_centre',
        'debit_balance_year_to_date',
        'analysis_code_2',
        'analysis_code_1',
        'analysis_code_4',
        'department_id',
        'analysis_code_3',
        'analysis_code_6',
        'analysis_code_5',
        'periods_to_keep_transactions',
        'analysis_code_8',
        'analysis_code_7',
        'date_time_updated',
        'cost_centre_id',
        'analysis_code_9',
        'analysis_code_10',
        'analysis_code_11',
        'analysis_code_12',
        'analysis_code_13',
        boolean_to_string('allow_manual_journal_entries'),
        'analysis_code_14',
        'analysis_code_15',
        'analysis_code_16',
        'analysis_code_17',
        'analysis_code_18',
        'analysis_code_19',
        adapter.quote('name'),
        'balance_year_to_date',
        'date_time_created',
        boolean_to_string('display_balances_in_selection_list'),
        'account_status_type',
        'credit_balance_year_to_date',
        'consolidated_nominal_account_number',
    ]) }} as _airbyte_nominal_codes_hashid,
    tmp.*
from {{ ref('nominal_codes_ab2') }} tmp
-- nominal_codes
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

