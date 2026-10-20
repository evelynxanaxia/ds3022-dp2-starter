-- This code is a dbt model that stages the raw fragments data by selecting distinct order numbers and words, casting the order number to an integer and trimming any whitespace from the word. 
-- The source data is pulled from the 'raw' schema and the 'fragments' table.
select distinct
    cast(order_no as integer) as order_no, 
    trim(word)                as word
from {{ source('raw', 'fragments') }}