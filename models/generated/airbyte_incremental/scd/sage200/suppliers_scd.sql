{{ config(
    indexes = [{'columns':['_airbyte_active_row','_airbyte_unique_key_scd','_airbyte_emitted_at'],'type': 'btree'}],
    unique_key = "_airbyte_unique_key_scd",
    schema = "_airbyte_sage200_etl_scd_pte",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='suppliers'
                        )
                    %}
                    {#
                    If the final table doesn't exist, then obviously we can't delete anything from it.
                    Also, after a reset, the final table is created without the _airbyte_unique_key column (this column is created during the first sync)
                    So skip this deletion if the column doesn't exist. (in this case, the table is guaranteed to be empty anyway)
                    #}
                    {%
                    if final_table_relation is not none and '_airbyte_unique_key' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    -- Delete records which are no longer active:
                    -- This query is equivalent, but the left join version is more performant:
                    -- delete from final_table where unique_key in (
                    --     select unique_key from scd_table where 1 = 1 <incremental_clause(normalized_at, final_table)>
                    -- ) and unique_key not in (
                    --     select unique_key from scd_table where active_row = 1 <incremental_clause(normalized_at, final_table)>
                    -- )
                    -- We're incremental against normalized_at rather than emitted_at because we need to fetch the SCD
                    -- entries that were _updated_ recently. This is because a deleted record will have an SCD record
                    -- which was emitted a long time ago, but recently re-normalized to have active_row = 0.
                    delete from {{ final_table_relation }} where {{ final_table_relation }}._airbyte_unique_key in (
                        select recent_records.unique_key
                        from (
                                select distinct _airbyte_unique_key as unique_key
                                from {{ this }}
                                where 1=1 {{ incremental_clause('_airbyte_normalized_at', adapter.quote(this.schema) + '.' + adapter.quote('suppliers')) }}
                            ) recent_records
                            left join (
                                select _airbyte_unique_key as unique_key, count(_airbyte_unique_key) as active_count
                                from {{ this }}
                                where _airbyte_active_row = 1 {{ incremental_clause('_airbyte_normalized_at', adapter.quote(this.schema) + '.' + adapter.quote('suppliers')) }}
                                group by _airbyte_unique_key
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ","delete from _airbyte_sage200_etl_stg_pte.suppliers_stg where _airbyte_emitted_at != (select max(_airbyte_emitted_at) from _airbyte_sage200_etl_stg_pte.suppliers_stg)"],
    tags = [ "top-level" ]
) }}
-- depends_on: ref('suppliers_stg')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('suppliers_stg')  }}
    -- suppliers from {{ source('sage200_etl_pte', '_airbyte_raw_suppliers') }}
    where 1 = 1
    {{ incremental_clause('_airbyte_emitted_at', this) }}
),
new_data_ids as (
    -- build a subset of _airbyte_unique_key from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            adapter.quote('id'),
        ]) }} as _airbyte_unique_key
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('suppliers_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._airbyte_unique_key = new_data_ids._airbyte_unique_key
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._airbyte_ab_id = inc_data._airbyte_ab_id
    where _airbyte_active_row = 1
),
input_data as (
    select {{ dbt_utils.star(ref('suppliers_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('suppliers_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('suppliers_stg')  }}
    -- suppliers from {{ source('sage200_etl_pte', '_airbyte_raw_suppliers') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
      adapter.quote('id'),
      ]) }} as _airbyte_unique_key,
      mainAddress_address_1,
	  mainAddress_address_2,
	  mainAddress_address_3,
	  mainAddress_address_4,
	  mainAddress_city,
	  mainAddress_county,
	  mainAddress_country,
	  mainAddress_code,
	  mainAddress_postcode,
      account_type,
      vat_number,
      telephone_subscriber_number,
      reference,
      balance,
      default_nominal_code_cost_centre,
      default_nominal_code_department,
      country_code_id,
      {{ adapter.quote('id') }},
      payment_terms_days,
      trading_terms,
      months_to_keep_transactions,
      spare_text_3,
      spare_number_7,
      spare_text_2,
      spare_number_6,
      spare_text_5,
      spare_number_9,
      analysis_code_2,
      payment_terms_basis,
      spare_text_4,
      spare_number_8,
      analysis_code_1,
      spare_text_7,
      analysis_code_4,
      spare_text_6,
      analysis_code_3,
      spare_text_9,
      analysis_code_6,
      spare_text_8,
      analysis_code_5,
      analysis_code_8,
      analysis_code_7,
      spare_number_1,
      analysis_code_9,
      spare_number_3,
      spare_number_2,
      spare_text_1,
      spare_number_5,
      spare_number_4,
      credit_bureau_id,
      order_priority,
      {{ adapter.quote('name') }},
      short_name,
      early_settlement_discount_percent,
      payment_group_id,
      spare_number_10,
      fax_subscriber_number,
      fax_country_code,
      credit_position_id,
      fax_area_code,
      analysis_code_20,
      telephone_area_code,
      early_settlement_discount_days,
      default_nominal_code_reference,
      factor_house_id,
      value_of_current_orders_in_pop,
      credit_limit,
      status_reason,
      website,
      credit_reference,
      telephone_country_code,
      date_time_updated,
      on_hold,
      analysis_code_10,
      analysis_code_11,
      analysis_code_12,
      analysis_code_13,
      is_supplier_payments_enabled,
      spare_bool_5,
      analysis_code_14,
      analysis_code_15,
      exchange_rate_type,
      spare_text_10,
      analysis_code_16,
      default_tax_code_id,
      analysis_code_17,
      analysis_code_18,
      analysis_code_19,
      spare_bool_3,
      date_time_created,
      spare_bool_4,
      currency_id,
      spare_bool_1,
      account_opened,
      spare_bool_2,
      terms_agreed,
      account_status_type,
      date_time_updated as _airbyte_start_at,
      lag(date_time_updated) over (
        partition by cast({{ adapter.quote('id') }} as {{ dbt_utils.type_string() }})
        order by
            date_time_updated is null asc,
            date_time_updated desc,
            _airbyte_emitted_at desc
      ) as _airbyte_end_at,
      case when row_number() over (
        partition by cast({{ adapter.quote('id') }} as {{ dbt_utils.type_string() }})
        order by
            date_time_updated is null asc,
            date_time_updated desc,
            _airbyte_emitted_at desc
      ) = 1 then 1 else 0 end as _airbyte_active_row,
      _airbyte_ab_id,
      _airbyte_emitted_at,
      _airbyte_suppliers_hashid
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by
                _airbyte_unique_key,
                _airbyte_start_at,
                _airbyte_emitted_at
            order by _airbyte_active_row desc, _airbyte_ab_id
        ) as _airbyte_row_num,
        {{ dbt_utils.surrogate_key([
          '_airbyte_unique_key',
          '_airbyte_start_at',
          '_airbyte_emitted_at'
        ]) }} as _airbyte_unique_key_scd,
        scd_data.*
    from scd_data
)
select
    _airbyte_unique_key,
    _airbyte_unique_key_scd,
    mainAddress_address_1,
	mainAddress_address_2,
	mainAddress_address_3,
	mainAddress_address_4,
	mainAddress_city,
	mainAddress_county,
	mainAddress_country,
	mainAddress_code,
	mainAddress_postcode,
    account_type,
    vat_number,
    telephone_subscriber_number,
    reference,
    balance,
    default_nominal_code_cost_centre,
    default_nominal_code_department,
    country_code_id,
    {{ adapter.quote('id') }},
    payment_terms_days,
    trading_terms,
    months_to_keep_transactions,
    spare_text_3,
    spare_number_7,
    spare_text_2,
    spare_number_6,
    spare_text_5,
    spare_number_9,
    analysis_code_2,
    payment_terms_basis,
    spare_text_4,
    spare_number_8,
    analysis_code_1,
    spare_text_7,
    analysis_code_4,
    spare_text_6,
    analysis_code_3,
    spare_text_9,
    analysis_code_6,
    spare_text_8,
    analysis_code_5,
    analysis_code_8,
    analysis_code_7,
    spare_number_1,
    analysis_code_9,
    spare_number_3,
    spare_number_2,
    spare_text_1,
    spare_number_5,
    spare_number_4,
    credit_bureau_id,
    order_priority,
    {{ adapter.quote('name') }},
    short_name,
    early_settlement_discount_percent,
    payment_group_id,
    spare_number_10,
    fax_subscriber_number,
    fax_country_code,
    credit_position_id,
    fax_area_code,
    analysis_code_20,
    telephone_area_code,
    early_settlement_discount_days,
    default_nominal_code_reference,
    factor_house_id,
    value_of_current_orders_in_pop,
    credit_limit,
    status_reason,
    website,
    credit_reference,
    telephone_country_code,
    date_time_updated,
    on_hold,
    analysis_code_10,
    analysis_code_11,
    analysis_code_12,
    analysis_code_13,
    is_supplier_payments_enabled,
    spare_bool_5,
    analysis_code_14,
    analysis_code_15,
    exchange_rate_type,
    spare_text_10,
    analysis_code_16,
    default_tax_code_id,
    analysis_code_17,
    analysis_code_18,
    analysis_code_19,
    spare_bool_3,
    date_time_created,
    spare_bool_4,
    currency_id,
    spare_bool_1,
    account_opened,
    spare_bool_2,
    terms_agreed,
    account_status_type,
    _airbyte_start_at,
    _airbyte_end_at,
    _airbyte_active_row,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_suppliers_hashid
from dedup_data where _airbyte_row_num = 1

