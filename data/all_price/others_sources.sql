insert into {{params.dataset}}.{{params.target_table}}
with tmp as (
    select date_add(price_date, interval 1 day) as price_date, wine_id, wine as wine_name, vintage, {{params.source}} as price_per_bottle, {{params.source}}_is_fillna as is_fillna, packing_size,
    case when lag({{params.source}}_is_fillna) over (partition by wine_id order by price_date) = {{params.source}}_is_fillna then 0 else 1 end as flag
    from {{params.dataset}}.{{params.source_table}}
    where {{params.source}} is not null and price_date >= date_sub(date('{{params.six_month_date}}'), interval 1 day)
), tmp2 as (
    select price_date, wine_id, wine_name, vintage, price_per_bottle, is_fillna, packing_size, sum(flag) OVER (PARTITION BY wine_id ORDER BY price_date) section
    from tmp
), tmp3 as (
    select price_date, wine_id, wine_name, vintage, packing_size, price_per_bottle, is_fillna, section, 
    case when is_fillna = true then ROW_NUMBER() OVER (PARTITION BY wine_id, section ORDER BY price_date) else 0 end as row_no_for_null
    from tmp2
), new_data as (
    select price_date, wine_id, wine_name, vintage, packing_size, price_per_bottle, is_fillna, section, row_no_for_null
    from tmp3
    where row_no_for_null is null or row_no_for_null <= 14
), max_date as (
    select wine_id, max(price_date) as price_date
    from trips_all_data.all_price
    where source = '{{params.source}}'
    group by 1
), existing_data as (
    select price_date, wine_id, row_no_for_null 
    from {{params.dataset}}.{{params.target_table}}
    where source = '{{params.source}}'
), max_existing_data as (
    select t2.wine_id, t2.price_date, t2.row_no_for_null
    from max_date t1
    join existing_data t2 on t1.price_date = t2.price_date and t1.wine_id = t2.wine_id
), cal_row_no_for_null as (
    select t1.price_date, t1.wine_id, t1.wine_name, t1.vintage, t1.packing_size, '{{params.source}}', t1.price_per_bottle, null as wwx_adjusted_price, t1.is_fillna,
    case when (14 - date_diff(t1.price_date, date_sub('{{params.six_month_date}}', interval 1 day), DAY) >= 0) and (t1.section = 1)
    then t1.row_no_for_null + coalesce(t2.row_no_for_null,0)
    else t1.row_no_for_null end as row_no_for_null
    from new_data t1
    left join max_existing_data t2 on t1.wine_id = t2.wine_id
)
select price_date, wine_id, wine_name, vintage, packing_size, '{{params.source}}', price_per_bottle, null as wwx_adjusted_price, is_fillna,
row_no_for_null, true as include_flag, date('{{params.batch_date}}'), current_timestamp
from cal_row_no_for_null
where row_no_for_null <= 14