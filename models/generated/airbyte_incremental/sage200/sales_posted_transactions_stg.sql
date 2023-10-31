{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sage200_etl_stg_pte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('sales_posted_transactions_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'base_allocated_value',
        'base_discount_value',
        'full_settlement_date',
        'user_name',
        'control_value_in_base_currency',
        'document_tax_value',
        'trader_transaction_type',
        'reference',
        'discount_days',
        adapter.quote('id'),
        'queried',
        'transaction_date',
        'exchange_rate',
        'base_tax_discount_value',
        'document_discount_value',
        'due_date',
        'date_time_updated',
        'base_outstanding_value',
        'document_allocated_value',
        'document_outstanding_value',
        'urn',
        'base_tax_value',
        'base_gross_value',
        'second_reference',
        boolean_to_string('settled_immediately'),
        'base_goods_value',
        'document_goods_value',
        'document_gross_value',
        'document_tax_discount_value',
        'customer_id',
        'discount_percent',
        'date_time_created',
        'posted_date',
    ]) }} as _airbyte_sales_posted_transactions_hashid,
    tmp.*
from {{ ref('sales_posted_transactions_ab2') }} tmp
-- sales_posted_transactions
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

