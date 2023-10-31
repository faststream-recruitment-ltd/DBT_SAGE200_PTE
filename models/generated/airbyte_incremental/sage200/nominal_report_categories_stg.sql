{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('nominal_report_categories_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'code',
        'account_report_category_type',
        'account_report_type',
        'description',
        'date_time_updated',
        adapter.quote('id'),
        'date_time_created',
    ]) }} as _airbyte_nominal_report_categories_hashid,
    tmp.*
from {{ ref('nominal_report_categories_ab2') }} tmp
-- nominal_report_categories
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

