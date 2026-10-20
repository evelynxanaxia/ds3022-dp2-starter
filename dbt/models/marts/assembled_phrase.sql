-- This code is a dbt model that assembles the phrase from the staged fragments data by counting the number of fragments and concatenating the words in order.
select
    count(*)                                as fragment_count,
    string_agg(word, ' ' order by order_no) as phrase
from {{ ref('stg_fragments') }}