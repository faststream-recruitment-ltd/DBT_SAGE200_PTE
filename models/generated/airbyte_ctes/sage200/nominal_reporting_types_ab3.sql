{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('nominal_reporting_types_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'description',
        adapter.quote('id'),
        adapter.quote('value'),
    ]) }} as _airbyte_nominal_reporting_types_hashid,
    tmp.*
from {{ ref('nominal_reporting_types_ab2') }} tmp
-- nominal_reporting_types
where 1 = 1

