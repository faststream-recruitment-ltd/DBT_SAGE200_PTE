{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('departments_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'contact_name',
        'code',
        'contact_email_address',
        adapter.quote('name'),
        'date_time_updated',
        'contact_details',
        adapter.quote('id'),
        'date_time_created',
    ]) }} as _airbyte_departments_hashid,
    tmp.*
from {{ ref('departments_ab2') }} tmp
-- departments
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

