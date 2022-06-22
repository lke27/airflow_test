insert into {{params.dataset}}.{{params.target_table}}
select t1.price_date,
t1.wine_id,
t1.wine_name,
t1.vintage,
t1.packing_size,
t1.source,
t1.price_per_bottle,
t1.wwx_adjusted_price,
t1.is_fillna,
t1.row_no_for_null,
case when t2.price_date is null then true else false end as include_flag,
date('{{params.batch_date}}'), 
current_timestamp
from {{params.dataset}}.{{params.working_table_1}} t1
left join {{params.dataset}}.{{params.working_table_3}} t2 
on t1.price_date = t2.price_date and t1.wine_id = t2.wine_id and t1.source = t2.source