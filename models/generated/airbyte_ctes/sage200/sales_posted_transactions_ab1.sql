{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sage200_etl_pte', '_airbyte_raw_sales_posted_transactions') }}
select
    {{ json_extract_scalar('_airbyte_data', ['base_allocated_value'], ['base_allocated_value']) }} as base_allocated_value,
    {{ json_extract_scalar('_airbyte_data', ['base_discount_value'], ['base_discount_value']) }} as base_discount_value,
    {{ json_extract_scalar('_airbyte_data', ['full_settlement_date'], ['full_settlement_date']) }} as full_settlement_date,
    {{ json_extract_scalar('_airbyte_data', ['user_name'], ['user_name']) }} as user_name,
    {{ json_extract_scalar('_airbyte_data', ['control_value_in_base_currency'], ['control_value_in_base_currency']) }} as control_value_in_base_currency,
    {{ json_extract_scalar('_airbyte_data', ['document_tax_value'], ['document_tax_value']) }} as document_tax_value,
    {{ json_extract_scalar('_airbyte_data', ['trader_transaction_type'], ['trader_transaction_type']) }} as trader_transaction_type,
    {{ json_extract_scalar('_airbyte_data', ['reference'], ['reference']) }} as reference,
    {{ json_extract_scalar('_airbyte_data', ['discount_days'], ['discount_days']) }} as discount_days,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['queried'], ['queried']) }} as queried,
    {{ json_extract_scalar('_airbyte_data', ['transaction_date'], ['transaction_date']) }} as transaction_date,
    {{ json_extract_scalar('_airbyte_data', ['exchange_rate'], ['exchange_rate']) }} as exchange_rate,
    {{ json_extract_scalar('_airbyte_data', ['base_tax_discount_value'], ['base_tax_discount_value']) }} as base_tax_discount_value,
    {{ json_extract_scalar('_airbyte_data', ['document_discount_value'], ['document_discount_value']) }} as document_discount_value,
    {{ json_extract_scalar('_airbyte_data', ['due_date'], ['due_date']) }} as due_date,
    {{ json_extract_scalar('_airbyte_data', ['date_time_updated'], ['date_time_updated']) }} as date_time_updated,
    {{ json_extract_scalar('_airbyte_data', ['base_outstanding_value'], ['base_outstanding_value']) }} as base_outstanding_value,
    {{ json_extract_scalar('_airbyte_data', ['document_allocated_value'], ['document_allocated_value']) }} as document_allocated_value,
    {{ json_extract_scalar('_airbyte_data', ['document_outstanding_value'], ['document_outstanding_value']) }} as document_outstanding_value,
    {{ json_extract_scalar('_airbyte_data', ['urn'], ['urn']) }} as urn,
    {{ json_extract_scalar('_airbyte_data', ['base_tax_value'], ['base_tax_value']) }} as base_tax_value,
    {{ json_extract_scalar('_airbyte_data', ['base_gross_value'], ['base_gross_value']) }} as base_gross_value,
    {{ json_extract_scalar('_airbyte_data', ['second_reference'], ['second_reference']) }} as second_reference,
    {{ json_extract_scalar('_airbyte_data', ['settled_immediately'], ['settled_immediately']) }} as settled_immediately,
    {{ json_extract_scalar('_airbyte_data', ['base_goods_value'], ['base_goods_value']) }} as base_goods_value,
    {{ json_extract_scalar('_airbyte_data', ['document_goods_value'], ['document_goods_value']) }} as document_goods_value,
    {{ json_extract_scalar('_airbyte_data', ['document_gross_value'], ['document_gross_value']) }} as document_gross_value,
    {{ json_extract_scalar('_airbyte_data', ['document_tax_discount_value'], ['document_tax_discount_value']) }} as document_tax_discount_value,
    {{ json_extract_scalar('_airbyte_data', ['customer_id'], ['customer_id']) }} as customer_id,
    {{ json_extract_scalar('_airbyte_data', ['discount_percent'], ['discount_percent']) }} as discount_percent,
    {{ json_extract_scalar('_airbyte_data', ['date_time_created'], ['date_time_created']) }} as date_time_created,
    {{ json_extract_scalar('_airbyte_data', ['posted_date'], ['posted_date']) }} as posted_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sage200_etl_pte', '_airbyte_raw_sales_posted_transactions') }} as table_alias
-- sales_posted_transactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

