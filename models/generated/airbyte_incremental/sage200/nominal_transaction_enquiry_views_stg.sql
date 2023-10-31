{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('nominal_transaction_enquiry_views_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'transaction_date',
        'transaction_analysis_code',
        'user_name',
        'narrative',
        'nominal_code_id',
        adapter.quote('source'),
        'period_number',
        'urn',
        'reference',
        'accounting_period_id',
        adapter.quote('id'),
        'debit',
        'credit',
        'year_relative_to_current_year',
        'financial_year_id',
        'financial_year_start_date',
    ]) }} as _airbyte_nominal_tra___enquiry_views_hashid,
    tmp.*
from {{ ref('nominal_transaction_enquiry_views_ab2') }} tmp
-- nominal_transaction_enquiry_views
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

