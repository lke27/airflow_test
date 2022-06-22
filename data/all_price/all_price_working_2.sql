INSERT INTO {{params.dataset}}.{{params.working_table}}
with source_cnt as (
    select price_date, wine_id, count(source) as cnt
    from {{params.dataset}}.{{params.target_table}}
	where price_date >= '{{params.six_month_date}}'
	group by price_date, wine_id
)
select t2.price_date, t2.wine_id, t2.source, t2.price_per_bottle, t1.cnt as source_cnt, date('{{params.batch_date}}'), current_timestamp
from source_cnt t1
join {{params.dataset}}.{{params.target_table}} t2 
on t1.price_date = t2.price_date and t1.wine_id = t2.wine_id