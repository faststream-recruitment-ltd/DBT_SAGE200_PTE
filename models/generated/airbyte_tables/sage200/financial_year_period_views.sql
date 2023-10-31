{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "sage200_etl_pte",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='financial_year_period_views_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('financial_year_period_views_ab3') }}
select
    period_end_date,
    number_of_periods_in_year,
    period_start_date,
    accounting_period_id,
    year_relative_to_current_year,
    period_number,
    financial_year_id,
    financial_year_start_date,
    financial_year_end_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_financial_year_period_views_hashid
from {{ ref('financial_year_period_views_ab3') }}
-- financial_year_period_views from {{ source('sage200_etl_pte', '_airbyte_raw_financial_year_period_views') }}
where 1 = 1

