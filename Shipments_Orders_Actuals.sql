-- create table toplinef_test.accheria_tableau_shipsorders_dashboard_references as 
-- select '2019-06-19'::DATE as dataset_date  ,'2019-06-19 25-Jun' as data_type_name, '2019 OP2' as fx_scenario union
-- select '2019-09-16'::DATE as dataset_date  ,'2019-09-16 20-Sept' as data_type_name, '2019 OP2' as fx_scenario  union 
-- select '2019-10-26'::DATE as dataset_date  ,'2019-10-26 01-Nov' as data_type_name, '2019 OP2' as fx_scenario union
-- select '2019-12-02'::DATE as dataset_date  ,'2019-12-02 02-Dec' as data_type_name, '2019 OP2' as fx_scenario;


with fx as (
select scenario,source_country_code as country_code,fx_rate,updated_date
from topline_fx_rates
where target_country_code = 'US'
and scenario = (select distinct fx_scenario from toplinef_test.accheria_tableau_shipsorders_dashboard_references limit 1)
and source_country_code in ('US','CA','UK','DE','FR','IT','ES','JP')),

dataset_dates as (
select dataset_date,data_type_name from toplinef_test.accheria_tableau_shipsorders_dashboard_references), 

ship_pids_final as (select distinct dataset_date,max(portfolio_id) portfolio_id from shipment_gl_forecasts_v where dataset_date in (select dataset_date from dataset_dates) group by 1),
orders_pids_final as (select dataset_date,max(forecast_id) as forecast_id from adjusted_orders_forecast where dataset_date in (select dataset_date from dataset_dates) group by 1),
orders_pids_system as (select dataset_date,max(forecast_id) as forecast_id from orders_gl_forecasts_daily where dataset_date in (select dataset_date from dataset_dates) group by 1)

--Orders Actuals
select order_day as tableau_refresh_date,NULL::DATE as dataset_date, 1 as source, 'Orders' as data_stream,
cast('Actuals' as varchar(40)) as data_type,country_code,channel,'' as subchannel,
'' as gl_rollup,'' as gl_product_group,'' as gl_name,prime_group,
order_day as target_day, EXTRACT(DOW FROM order_day)+1 as day_of_week,
date_trunc('week', order_day +1)::DATE-1 as target_week, 
to_char(order_day + interval '1 day', 'IW') as week_number,EXTRACT(year FROM date_trunc('week', order_day +1)::DATE -1 +6) as Year_of_week,
date_trunc('month', order_day) as target_month, EXTRACT(month FROM order_day) as month_number,
EXTRACT(year FROM order_day::DATE) as year_number,
sum(net_ordered_units) as units,sum(case when channel = 'ret' then net_ops*fx_rate else net_ordered_gms*fx_rate end) as prv_gms_USD,
sum(net_ordered_gms*fx_rate) as GMS_USD,sum(net_ops*fx_rate) as PRV_USD
from  order_actuals_osa_v
join fx using (country_code)
where extract(year from order_day) > 2013
and gl_rollup not in ('dig','unk') and gl_product_group < 990
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

union

--Shipments Actuals
select target_day::DATE as tableau_refresh_date,NULL::DATE as dataset_date, 1 as source, 'Shipments' as data_stream,
cast('Actuals' as varchar(40)) as data_type,country_code,channel,subchannel,
'' as gl_rollup,'' as gl_product_group,'' as gl_name,prime_group,
target_day, EXTRACT(DOW FROM target_day::DATE)+1 as day_of_week,
target_week, to_char(target_day + interval '1 day', 'IW') as week_number,EXTRACT(year FROM target_week::DATE+6) as Year_of_week,
target_month, EXTRACT(month FROM target_month::DATE) as month_number,
EXTRACT(year FROM target_month::DATE) as year_number,
sum(shipped_units) as units,sum(case when channel = 'ret' then product_revenue_usd else gross_gms_usd end) as prv_gms_USD,
sum(gross_gms_usd) as GMS_USD,sum(product_revenue_usd) as PRV_USD
from SHIPMENT_GL_ACTUALS_ALL_V 
where dataset_date = (select max(dataset_date) from SHIPMENT_GL_ACTUALS_ALL_V)
and extract(year from target_day) > 2013
and gl_rollup not in ('dig','unk') and gl_product_group < 990
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

union 

--Shipments Final Forecast
select dataset_date::DATE  - 10000 as tableau_refresh_date,dataset_date::DATE,1 as source, 
'Shipments' as data_stream,
cast(data_type_name + ' Final Forecast' as varchar(40)) as data_type,
country_code,channel,subchannel,'' as gl_rollup,'' as gl_product_group,'' as gl_name,prime_group,
target_day, EXTRACT(DOW FROM target_day::DATE)+1 as day_of_week,
target_week, to_char(target_day + interval '1 day', 'IW') as week_number,EXTRACT(year FROM target_week::DATE+6) as Year_of_week,
target_month, EXTRACT(month FROM target_month::DATE) as month_number,
EXTRACT(year FROM target_month::DATE) as year_number,
sum(shipped_units) as units,sum(case when channel = 'ret' then product_revenue_usd else gross_gms_usd end) as prv_gms_USD,
sum(gross_gms_usd) as GMS_USD,sum(product_revenue_usd) as PRV_USD
from shipment_gl_forecasts_v 
join ship_pids_final using (portfolio_id,dataset_date)
join dataset_dates using (dataset_date)
where target_day >= date_trunc('week',dataset_date+2)::DATE - 1
and extract(year from target_day) < 2021
and gl_rollup not in ('dig','unk') and gl_product_group < 990
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

union

--Shipments System Forecast
select dataset_date::DATE  - 10000 as tableau_refresh_date,dataset_date::DATE,1 as source, 
'Shipments' as data_stream,
  cast(data_type_name + ' System Forecast' as varchar(40)) as data_type,
country_code,channel,subchannel,'' as gl_rollup,'' as gl_product_group,'' as gl_name,prime_group,
target_day, EXTRACT(DOW FROM target_day::DATE)+1 as day_of_week,
target_week, to_char(target_day + interval '1 day', 'IW') as week_number,EXTRACT(year FROM target_week::DATE+6) as Year_of_week,
target_month, EXTRACT(month FROM target_month::DATE) as month_number,
EXTRACT(year FROM target_month::DATE) as year_number,
sum(shipped_units) as units,sum(case when channel = 'ret' then product_revenue_usd else gross_gms_usd end) as prv_gms_USD,
sum(gross_gms_usd) as GMS_USD,sum(product_revenue_usd) as PRV_USD
from shipment_gl_forecasts_v 
join ship_pids_final using (portfolio_id,dataset_date)
join dataset_dates using (dataset_date)
where target_day >= date_trunc('week',dataset_date+2)::DATE - 1
and extract(year from target_day) < 2021
and gl_rollup not in ('dig','unk') and gl_product_group < 990
and initiative_type in ('Baseline')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

union

--Orders Final Forecast
select dataset_date::DATE  - 10000 as tableau_refresh_date,dataset_date::DATE,1 as source, 
'Orders'as data_stream,
cast(data_type_name + ' Final Forecast' as varchar(40)) as data_type,
country_code,channel,'' as subchannel,'' as gl_rollup,'' as gl_product_group,'' as gl_name,prime_group,
target_day::DATE as target_day, EXTRACT(DOW FROM target_day::DATE)+1 as day_of_week,
date_trunc('week', target_day +1)::DATE-1 as target_week, 
to_char(target_day + interval '1 day', 'IW') as week_number,EXTRACT(year FROM date_trunc('week', target_day +1)::DATE -1 +6) as Year_of_week,
date_trunc('month', target_day) as target_month, EXTRACT(month FROM target_day::DATE) as month_number,
EXTRACT(year FROM target_day::DATE) as year_number,
sum(net_ordered_units) as units,sum(case when channel = 'ret' then net_ops*fx_rate else net_ordered_gms*fx_rate end) as prv_gms_USD,
sum(net_ordered_gms*fx_rate) as GMS_USD,sum(net_ops*fx_rate) as PRV_USD
from adjusted_orders_forecast
join  orders_pids_final using (forecast_id,dataset_date)
join fx using (country_code)
join dataset_dates using (dataset_date)
where target_day >= date_trunc('week',dataset_date+2)::DATE - 1 and
extract(year from target_day) < 2021
and gl_rollup not in ('dig','unk') and gl_product_group < 990
and forecast_type = 'Adjusted'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

union

--Orders System Forecast
select dataset_date::DATE - 10000 as tableau_refresh_date,dataset_date::DATE, 1 as source, 'Orders' as data_stream,
cast(data_type_name + ' System Forecast' as varchar(40)) as data_type,
country_code,channel,'' as subchannel,'' as gl_rollup,'' as gl_product_group,'' as gl_name,prime_group,
target_day as target_day, EXTRACT(DOW FROM target_day::DATE)+1 as day_of_week,
date_trunc('week', target_day +1)::DATE-1 as target_week, 
to_char(target_day + interval '1 day', 'IW') as week_number,EXTRACT(year FROM date_trunc('week', target_day +1)::DATE -1 +6) as Year_of_week,
date_trunc('month', target_day) as target_month, EXTRACT(month FROM target_day::DATE) as month_number,
EXTRACT(year FROM target_day::DATE) as year_number,
sum(net_ordered_units) as units,sum(case when channel = 'ret' then net_ops*fx_rate else net_ordered_gms*fx_rate end) as prv_gms_USD,
sum(net_ordered_gms*fx_rate) as GMS_USD,sum(net_ops*fx_rate) as PRV_USD
from orders_gl_forecasts_daily
join  orders_pids_system using (forecast_id,dataset_date)
join fx using (country_code)
join dataset_dates using (dataset_date)
where target_day >= date_trunc('week',dataset_date+2)::DATE - 1 and
extract(year from target_day) < 2021
and gl_rollup not in ('dig','unk') and gl_product_group < 990
and forecast_type = 'Baseline'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20