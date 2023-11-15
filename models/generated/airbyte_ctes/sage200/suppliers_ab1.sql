{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sage200_etl_pte', '_airbyte_raw_suppliers') }}
select
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'address_1'], ['mainAddress_address_1']) }} as mainAddress_address_1,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'address_2'], ['mainAddress_address_2']) }} as mainAddress_address_2,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'address_3'], ['mainAddress_address_3']) }} as mainAddress_address_3,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'address_4'], ['mainAddress_address_4']) }} as mainAddress_address_4,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'city'], ['mainAddress_city']) }} as mainAddress_city,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'county'], ['mainAddress_county']) }} as mainAddress_county,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'address_country_code', 'name'], ['mainAddress_country']) }} as mainAddress_country,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'address_country_code', 'code'], ['mainAddress_code']) }} as mainAddress_code,
    {{ json_extract_scalar('_airbyte_data', ['main_address', 'postcode'], ['mainAddress_postcode']) }} as mainAddress_postcode,
    {{ json_extract_scalar('_airbyte_data', ['account_type'], ['account_type']) }} as account_type,
    {{ json_extract_scalar('_airbyte_data', ['vat_number'], ['vat_number']) }} as vat_number,
    {{ json_extract_scalar('_airbyte_data', ['telephone_subscriber_number'], ['telephone_subscriber_number']) }} as telephone_subscriber_number,
    {{ json_extract_scalar('_airbyte_data', ['reference'], ['reference']) }} as reference,
    {{ json_extract_scalar('_airbyte_data', ['balance'], ['balance']) }} as balance,
    {{ json_extract_scalar('_airbyte_data', ['default_nominal_code_cost_centre'], ['default_nominal_code_cost_centre']) }} as default_nominal_code_cost_centre,
    {{ json_extract_scalar('_airbyte_data', ['default_nominal_code_department'], ['default_nominal_code_department']) }} as default_nominal_code_department,
    {{ json_extract_scalar('_airbyte_data', ['country_code_id'], ['country_code_id']) }} as country_code_id,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['payment_terms_days'], ['payment_terms_days']) }} as payment_terms_days,
    {{ json_extract_scalar('_airbyte_data', ['trading_terms'], ['trading_terms']) }} as trading_terms,
    {{ json_extract_scalar('_airbyte_data', ['months_to_keep_transactions'], ['months_to_keep_transactions']) }} as months_to_keep_transactions,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_3'], ['spare_text_3']) }} as spare_text_3,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_7'], ['spare_number_7']) }} as spare_number_7,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_2'], ['spare_text_2']) }} as spare_text_2,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_6'], ['spare_number_6']) }} as spare_number_6,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_5'], ['spare_text_5']) }} as spare_text_5,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_9'], ['spare_number_9']) }} as spare_number_9,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_2'], ['analysis_code_2']) }} as analysis_code_2,
    {{ json_extract_scalar('_airbyte_data', ['payment_terms_basis'], ['payment_terms_basis']) }} as payment_terms_basis,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_4'], ['spare_text_4']) }} as spare_text_4,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_8'], ['spare_number_8']) }} as spare_number_8,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_1'], ['analysis_code_1']) }} as analysis_code_1,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_7'], ['spare_text_7']) }} as spare_text_7,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_4'], ['analysis_code_4']) }} as analysis_code_4,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_6'], ['spare_text_6']) }} as spare_text_6,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_3'], ['analysis_code_3']) }} as analysis_code_3,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_9'], ['spare_text_9']) }} as spare_text_9,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_6'], ['analysis_code_6']) }} as analysis_code_6,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_8'], ['spare_text_8']) }} as spare_text_8,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_5'], ['analysis_code_5']) }} as analysis_code_5,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_8'], ['analysis_code_8']) }} as analysis_code_8,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_7'], ['analysis_code_7']) }} as analysis_code_7,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_1'], ['spare_number_1']) }} as spare_number_1,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_9'], ['analysis_code_9']) }} as analysis_code_9,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_3'], ['spare_number_3']) }} as spare_number_3,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_2'], ['spare_number_2']) }} as spare_number_2,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_1'], ['spare_text_1']) }} as spare_text_1,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_5'], ['spare_number_5']) }} as spare_number_5,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_4'], ['spare_number_4']) }} as spare_number_4,
    {{ json_extract_scalar('_airbyte_data', ['credit_bureau_id'], ['credit_bureau_id']) }} as credit_bureau_id,
    {{ json_extract_scalar('_airbyte_data', ['order_priority'], ['order_priority']) }} as order_priority,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['short_name'], ['short_name']) }} as short_name,
    {{ json_extract_scalar('_airbyte_data', ['early_settlement_discount_percent'], ['early_settlement_discount_percent']) }} as early_settlement_discount_percent,
    {{ json_extract_scalar('_airbyte_data', ['payment_group_id'], ['payment_group_id']) }} as payment_group_id,
    {{ json_extract_scalar('_airbyte_data', ['spare_number_10'], ['spare_number_10']) }} as spare_number_10,
    {{ json_extract_scalar('_airbyte_data', ['fax_subscriber_number'], ['fax_subscriber_number']) }} as fax_subscriber_number,
    {{ json_extract_scalar('_airbyte_data', ['fax_country_code'], ['fax_country_code']) }} as fax_country_code,
    {{ json_extract_scalar('_airbyte_data', ['credit_position_id'], ['credit_position_id']) }} as credit_position_id,
    {{ json_extract_scalar('_airbyte_data', ['fax_area_code'], ['fax_area_code']) }} as fax_area_code,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_20'], ['analysis_code_20']) }} as analysis_code_20,
    {{ json_extract_scalar('_airbyte_data', ['telephone_area_code'], ['telephone_area_code']) }} as telephone_area_code,
    {{ json_extract_scalar('_airbyte_data', ['early_settlement_discount_days'], ['early_settlement_discount_days']) }} as early_settlement_discount_days,
    {{ json_extract_scalar('_airbyte_data', ['default_nominal_code_reference'], ['default_nominal_code_reference']) }} as default_nominal_code_reference,
    {{ json_extract_scalar('_airbyte_data', ['factor_house_id'], ['factor_house_id']) }} as factor_house_id,
    {{ json_extract_scalar('_airbyte_data', ['value_of_current_orders_in_pop'], ['value_of_current_orders_in_pop']) }} as value_of_current_orders_in_pop,
    {{ json_extract_scalar('_airbyte_data', ['credit_limit'], ['credit_limit']) }} as credit_limit,
    {{ json_extract_scalar('_airbyte_data', ['status_reason'], ['status_reason']) }} as status_reason,
    {{ json_extract_scalar('_airbyte_data', ['website'], ['website']) }} as website,
    {{ json_extract_scalar('_airbyte_data', ['credit_reference'], ['credit_reference']) }} as credit_reference,
    {{ json_extract_scalar('_airbyte_data', ['telephone_country_code'], ['telephone_country_code']) }} as telephone_country_code,
    {{ json_extract_scalar('_airbyte_data', ['date_time_updated'], ['date_time_updated']) }} as date_time_updated,
    {{ json_extract_scalar('_airbyte_data', ['on_hold'], ['on_hold']) }} as on_hold,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_10'], ['analysis_code_10']) }} as analysis_code_10,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_11'], ['analysis_code_11']) }} as analysis_code_11,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_12'], ['analysis_code_12']) }} as analysis_code_12,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_13'], ['analysis_code_13']) }} as analysis_code_13,
    {{ json_extract_scalar('_airbyte_data', ['is_supplier_payments_enabled'], ['is_supplier_payments_enabled']) }} as is_supplier_payments_enabled,
    {{ json_extract_scalar('_airbyte_data', ['spare_bool_5'], ['spare_bool_5']) }} as spare_bool_5,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_14'], ['analysis_code_14']) }} as analysis_code_14,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_15'], ['analysis_code_15']) }} as analysis_code_15,
    {{ json_extract_scalar('_airbyte_data', ['exchange_rate_type'], ['exchange_rate_type']) }} as exchange_rate_type,
    {{ json_extract_scalar('_airbyte_data', ['spare_text_10'], ['spare_text_10']) }} as spare_text_10,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_16'], ['analysis_code_16']) }} as analysis_code_16,
    {{ json_extract_scalar('_airbyte_data', ['default_tax_code_id'], ['default_tax_code_id']) }} as default_tax_code_id,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_17'], ['analysis_code_17']) }} as analysis_code_17,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_18'], ['analysis_code_18']) }} as analysis_code_18,
    {{ json_extract_scalar('_airbyte_data', ['analysis_code_19'], ['analysis_code_19']) }} as analysis_code_19,
    {{ json_extract_scalar('_airbyte_data', ['spare_bool_3'], ['spare_bool_3']) }} as spare_bool_3,
    {{ json_extract_scalar('_airbyte_data', ['date_time_created'], ['date_time_created']) }} as date_time_created,
    {{ json_extract_scalar('_airbyte_data', ['spare_bool_4'], ['spare_bool_4']) }} as spare_bool_4,
    {{ json_extract_scalar('_airbyte_data', ['currency_id'], ['currency_id']) }} as currency_id,
    {{ json_extract_scalar('_airbyte_data', ['spare_bool_1'], ['spare_bool_1']) }} as spare_bool_1,
    {{ json_extract_scalar('_airbyte_data', ['account_opened'], ['account_opened']) }} as account_opened,
    {{ json_extract_scalar('_airbyte_data', ['spare_bool_2'], ['spare_bool_2']) }} as spare_bool_2,
    {{ json_extract_scalar('_airbyte_data', ['terms_agreed'], ['terms_agreed']) }} as terms_agreed,
    {{ json_extract_scalar('_airbyte_data', ['account_status_type'], ['account_status_type']) }} as account_status_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sage200_etl_pte', '_airbyte_raw_suppliers') }} as table_alias
-- suppliers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

