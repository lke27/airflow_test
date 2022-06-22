INSERT INTO {{params.dataset}}.{{params.working_table}}
with source as (
    select price_date, wine_id, source, price_per_bottle as price
    from {{params.dataset}}.{{params.source_table}}
    where source = '{{params.source}}' and source_cnt >= 5
), other as (
    select price_date, wine_id, avg(price_per_bottle) as price
    from {{params.dataset}}.{{params.source_table}}
    where source <> '{{params.source}}' and source_cnt >= 5
    group by price_date, wine_id
)
select t1.price_date, t1.wine_id, t1.source, date('{{params.batch_date}}'), current_timestamp
from source t1
left join other t2 on t1.price_date = t2.price_date and t1.wine_id = t2.wine_id
where round(t2.price/t1.price - 1, 2) > 0.1