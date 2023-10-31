{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "sage200_etl_pte",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('purchase_posted_transactions_scd') }}
select
    _airbyte_unique_key,
    base_allocated_value,
    base_discount_value,
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
    discount_percent,
    date_time_created,
    supplier_id,
    posted_date,
    vat_adjustment_doc_expected,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_purchase_po__d_transactions_hashid
from {{ ref('purchase_posted_transactions_scd') }}
-- purchase_posted_transactions from {{ source('sage200_etl_pte', '_airbyte_raw_purchase_posted_transactions') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

