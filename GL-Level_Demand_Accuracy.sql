with gl_list as (
select gl_product_group,max(gl_name) as gl_name
from topline_master_gl_list
where gl_name not in ('Unknown')
group by 1),
fcst as (
select dataset_date
, country_code
, channel
, gl_rollup
, gl_product_group
, right(to_char(target_day + interval '1 day', 'IYYY-IW'),2)::int as week
, target_day
, sum(net_ordered_units) as fcst_units
, sum(case when channel = 'ret' then net_ops * fx_rate else net_ordered_gms * fx_rate end) as fcst_gms
from adjusted_orders_forecast a
join topline_fx_rates b
on a.country_code = b.source_country_code
and b.scenario = '2019 OP2'
where
forecast_id = 6106
and gl_rollup not in ('dig','unk')
and gl_product_group < 991
and country_code in ('US','CA','UK','DE','FR','IT','ES','JP')
-- and right(to_char(target_day + interval '1 day', 'IYYY-IW'),2)::int between 1 and 9
-- and target_day <= '2020-02-26'
-- and left(to_char(target_day + interval '1 day', 'IYYY-IW'),4)::int = 2020
and target_day >= '2019-11-01'
group by 1,2,3,4,5,6,7
)
, actuals as 
(
select country_code
, channel
, gl_rollup
, gl_product_group
, order_day as target_day
, right(to_char(order_day + interval '1 day', 'IYYY-IW'),2)::int as week
, left(to_char(order_day + interval '1 day', 'IYYY-IW'),4)::int as year
, extract(dow from order_day) as dow
, sum(net_ordered_units) as act_units
, sum(case when channel = 'ret' then net_ops * fx_rate else net_ordered_gms * fx_rate end) as act_gms
from order_actuals_osa_v_q4_test a
join topline_fx_rates b
on a.country_code = b.source_country_code
and b.scenario = '2019 OP2'
where gl_rollup not in ('dig','unk')
and gl_product_group < 991
and country_code in ('US','CA','UK','DE','FR','IT','ES','JP')
-- and right(to_char(order_day + interval '1 day', 'IYYY-IW'),2)::int between 1 and 9
-- and target_day <= '2020-02-26'
-- and left(to_char(target_day + interval '1 day', 'IYYY-IW'),4)::int = 2020
and target_day >= '2019-11-01'
group by 1,2,3,4,5,6,7,8
)
, actuals_py as 
(
select country_code
, channel
, gl_rollup
, gl_product_group
, order_day as target_day
, right(to_char(order_day + interval '1 day', 'IYYY-IW'),2)::int as week
, left(to_char(order_day + interval '1 day', 'IYYY-IW'),4)::int+1 as year
, extract(dow from order_day) as dow
, sum(net_ordered_units) as act_py_units
, sum(case when channel = 'ret' then net_ops * fx_rate else net_ordered_gms * fx_rate end) as act_py_gms
from order_actuals_osa_v_q4_test a
join topline_fx_rates b
on a.country_code = b.source_country_code
and b.scenario = '2019 OP2'
where gl_rollup not in ('dig','unk')
and gl_product_group < 991
and country_code in ('US','CA','UK','DE','FR','IT','ES','JP')
-- and right(to_char(order_day + interval '1 day', 'IYYY-IW'),2)::int between 1 and 9
-- and target_day <= '2020-02-26'
-- and left(to_char(target_day + interval '1 day', 'IYYY-IW'),4)::int = 2020
group by 1,2,3,4,5,6,7,8)
select 'Orders' as data_type, f.*, act_units, act_gms,act_py_units,act_py_gms,gl_name
from fcst f 
join actuals a using (country_code, channel, gl_rollup,gl_product_group, target_day,week)
join gl_list using (gl_product_group)
left join actuals_py a_py using (country_code, channel, gl_rollup,gl_product_group,week,year,dow)