{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "sage200_etl_pte",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('currency_scd') }}
select
    _airbyte_unique_key,
    use_for_new_accounts,
    symbol,
    date_time_updated,
    currency_iso_code_id,
    exchange_rate_amendability_type,
    euro_currency_rate,
    core_currency_rate,
    exchange_rate_type,
    {{ adapter.quote('name') }},
    {{ adapter.quote('id') }},
    is_base_currency,
    is_euro_currency,
    date_time_created,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_currency_hashid
from {{ ref('currency_scd') }}
-- currency from {{ source('sage200_etl_pte', '_airbyte_raw_currency') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

