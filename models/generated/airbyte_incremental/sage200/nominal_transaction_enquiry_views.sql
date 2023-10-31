{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "sage200_etl_pte",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('nominal_transaction_enquiry_views_scd') }}
select
    _airbyte_unique_key,
    transaction_date,
    transaction_analysis_code,
    user_name,
    narrative,
    nominal_code_id,
    {{ adapter.quote('source') }},
    period_number,
    urn,
    reference,
    accounting_period_id,
    {{ adapter.quote('id') }},
    debit,
    credit,
    year_relative_to_current_year,
    financial_year_id,
    financial_year_start_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_nominal_tra___enquiry_views_hashid
from {{ ref('nominal_transaction_enquiry_views_scd') }}
-- nominal_transaction_enquiry_views from {{ source('sage200_etl_pte', '_airbyte_raw_nominal_transaction_enquiry_views') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

