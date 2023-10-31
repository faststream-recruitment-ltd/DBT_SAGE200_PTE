{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "sage200_etl_pte",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('cost_centres_scd') }}
select
    _airbyte_unique_key,
    contact_name,
    code,
    contact_email_address,
    {{ adapter.quote('name') }},
    date_time_updated,
    contact_details,
    {{ adapter.quote('id') }},
    date_time_created,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_cost_centres_hashid
from {{ ref('cost_centres_scd') }}
-- cost_centres from {{ source('sage200_etl_pte', '_airbyte_raw_cost_centres') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

