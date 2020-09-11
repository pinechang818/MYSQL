with gl_list as (
select gl_product_group,max(gl_name) as gl_name
from topline_master_gl_list
where gl_name not in ('Unknown')
group by 1),
act as (
  select 
    country_code,channel,gl_product_group,gl_rollup,order_day,
    sum(net_ordered_units) as ordered_units_act,
    sum(case when channel = 'ret' then net_ops * fx_rate else net_ordered_gms * fx_rate end) as gms_act
  from order_actuals_osa_v_q4_test a
  join topline_fx_rates b
  on a.country_code = b.source_country_code
  and b.scenario = '2019 OP2'
  where 
    gl_product_group < 990 and
    gl_rollup not in ('dig','unk') and 
    extract(year from order_day) = 2020
  group by 1,2,3,4,5),
max_act as (
  select country_code,max(order_day) as max_order_day from act group by 1),
fcst as (
  select 
    dataset_date,
    country_code,channel,gl_product_group,gl_rollup,target_day as order_day,
    sum(net_ordered_units) as ordered_units_fcst,
    sum(case when channel = 'ret' then net_ops * fx_rate else net_ordered_gms * fx_rate end) as gms_fcst
  from adjusted_orders_forecast a
  join topline_fx_rates b
  on a.country_code = b.source_country_code
  and b.scenario = '2019 OP2'
  where ((forecast_id = 5967 and country_code = 'JP') 
    or (forecast_id = 6106 and country_code not in ('JP')))
    and gl_product_group < 990 and
    gl_rollup not in ('dig','unk') and 
    extract(year from order_day) = 2020
  group by 1,2,3,4,5,6)
select * ,
case when nvl(gl_product_group, 0)::int in (121,194,328) then 'medical_supply'
when nvl(gl_product_group, 0)::int in (75, 325, 364, 370, 467, 510, 199) then 'other_consumables'
when nvl(gl_product_group, 0)::int in (229, 14, 63, 267, 201, 200, 196, 74, 60, 23, 15, 147, 21, 504, 261) then 'wfh_essentials'
else 'others' end as product_bucket
from fcst
left join act using (country_code,channel,gl_product_group,gl_rollup,order_day)
join max_act using (country_code)
left join gl_list using (gl_product_group)
where order_day <= max_order_day