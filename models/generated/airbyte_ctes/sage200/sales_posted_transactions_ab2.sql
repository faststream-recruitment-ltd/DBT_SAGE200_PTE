{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('sales_posted_transactions_ab1') }}
select
    cast(base_allocated_value as {{ dbt_utils.type_float() }}) as base_allocated_value,
    cast(base_discount_value as {{ dbt_utils.type_float() }}) as base_discount_value,
    cast(full_settlement_date as timestamp) as full_settlement_date,
    cast(user_name as {{ dbt_utils.type_string() }}) as user_name,
    cast(control_value_in_base_currency as {{ dbt_utils.type_float() }}) as control_value_in_base_currency,
    cast(document_tax_value as {{ dbt_utils.type_float() }}) as document_tax_value,
    cast(trader_transaction_type as {{ dbt_utils.type_string() }}) as trader_transaction_type,
    cast(reference as {{ dbt_utils.type_string() }}) as reference,
    cast(discount_days as {{ dbt_utils.type_float() }}) as discount_days,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('id') }},
    cast(queried as {{ dbt_utils.type_string() }}) as queried,
    cast(transaction_date as timestamp) as transaction_date,
    cast(exchange_rate as {{ dbt_utils.type_float() }}) as exchange_rate,
    cast(base_tax_discount_value as {{ dbt_utils.type_float() }}) as base_tax_discount_value,
    cast(document_discount_value as {{ dbt_utils.type_float() }}) as document_discount_value,
    cast(due_date as timestamp) as due_date,
    cast(date_time_updated as timestamp) as date_time_updated,
    cast(base_outstanding_value as {{ dbt_utils.type_float() }}) as base_outstanding_value,
    cast(document_allocated_value as {{ dbt_utils.type_float() }}) as document_allocated_value,
    cast(document_outstanding_value as {{ dbt_utils.type_float() }}) as document_outstanding_value,
    cast(urn as {{ dbt_utils.type_float() }}) as urn,
    cast(base_tax_value as {{ dbt_utils.type_float() }}) as base_tax_value,
    cast(base_gross_value as {{ dbt_utils.type_float() }}) as base_gross_value,
    cast(second_reference as {{ dbt_utils.type_string() }}) as second_reference,
    {{ cast_to_boolean('settled_immediately') }} as settled_immediately,
    cast(base_goods_value as {{ dbt_utils.type_float() }}) as base_goods_value,
    cast(document_goods_value as {{ dbt_utils.type_float() }}) as document_goods_value,
    cast(document_gross_value as {{ dbt_utils.type_float() }}) as document_gross_value,
    cast(document_tax_discount_value as {{ dbt_utils.type_float() }}) as document_tax_discount_value,
    cast(customer_id as {{ dbt_utils.type_float() }}) as customer_id,
    cast(discount_percent as {{ dbt_utils.type_float() }}) as discount_percent,
    cast(date_time_created as timestamp) as date_time_created,
    cast(posted_date as timestamp) as posted_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('sales_posted_transactions_ab1') }}
-- sales_posted_transactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

