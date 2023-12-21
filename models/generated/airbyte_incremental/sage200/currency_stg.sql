{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('currency_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        boolean_to_string('use_for_new_accounts'),
        'symbol',
        'date_time_updated',
        'currency_iso_code_id',
        'exchange_rate_amendability_type',
        'euro_currency_rate',
        'core_currency_rate',
        'exchange_rate_type',
        adapter.quote('name'),
        adapter.quote('id'),
        boolean_to_string('is_base_currency'),
        boolean_to_string('is_euro_currency'),
        'date_time_created',
    ]) }} as _airbyte_currency_hashid,
    tmp.*
from {{ ref('currency_ab2') }} tmp
-- currency
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

