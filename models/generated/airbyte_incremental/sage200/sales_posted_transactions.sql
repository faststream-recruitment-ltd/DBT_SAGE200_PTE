{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "sage200_etl_pte",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('sales_posted_transactions_scd') }}
select
    _airbyte_unique_key,
    base_allocated_value,
    base_discount_value,
    full_settlement_date,
    user_name,
    control_value_in_base_currency,
    document_tax_value,
    trader_transaction_type,
    reference,
    discount_days,
    {{ adapter.quote('id') }},
    queried,
    transaction_date,
    exchange_rate,
    base_tax_discount_value,
    document_discount_value,
    due_date,
    date_time_updated,
    base_outstanding_value,
    document_allocated_value,
    document_outstanding_value,
    urn,
    base_tax_value,
    base_gross_value,
    second_reference,
    settled_immediately,
    base_goods_value,
    document_goods_value,
    document_gross_value,
    document_tax_discount_value,
    customer_id,
    discount_percent,
    date_time_created,
    posted_date,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_sales_posted_transactions_hashid
from {{ ref('sales_posted_transactions_scd') }}
-- sales_posted_transactions from {{ source('sage200_etl_pte', '_airbyte_raw_sales_posted_transactions') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

