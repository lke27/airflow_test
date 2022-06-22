insert into {{params.dataset}}.{{params.working_table}}
select price_date, wine_id, wine_name, vintage, packing_size, source, price_per_bottle, wwx_adjusted_price, is_fillna, row_no_for_null, date('{{params.batch_date}}'), current_timestamp
from {{params.dataset}}.{{params.target_table}}
where price_date >= '{{params.six_month_date}}'