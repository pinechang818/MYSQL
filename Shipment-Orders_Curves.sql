with fx_rates as 
(
select * from topline_fx_rates where scenario = (select scenario from toplinef_test.tableau_fx_metadata_integ)
)
,
pids as (
  select distinct portfolio_id as "portfolio_id" from 
  (
    select portfolio_id from toplinef_test.tableau_portfolio_metadata_integ
    union
    -- last 5 PIT
    select portfolio_id from (
        select portfolio_id, rank() over (order by dataset_date desc) as "rank"
        from (
            select distinct portfolio_id, dataset_date from shipment_gl_forecasts_all_v where dataset_date >= '2019-06-01' and portfolio_status = 'TEST' )
    )
    where rank <= 5
    union
    -- last 5 production
    select portfolio_id from (
        select portfolio_id, rank() over (order by dataset_date desc) as "rank"
        from (
            select distinct portfolio_id, dataset_date from shipment_gl_forecasts_v where dataset_date >= '2019-06-01')
    )
    where rank <= 5
    union
    -- last 1 draft
    select portfolio_id from (
        select portfolio_id, rank() over (order by dataset_date desc) as "rank"
        from (
            select distinct portfolio_id, dataset_date from shipment_gl_forecasts_all_v where dataset_date >= '2019-06-01' and portfolio_status = 'DRAFT')
    )
    where rank = 1
    )
)
select
initiative as "initiative"
, 'shipments' as "type"
, portfolio_id
, country_code
, channel
, gl_rollup
, target_day
, sum(shipped_units) as "shu"
, sum(product_revenue_amt * fx_rate) as "prv"
, sum(gross_gms_amt * fx_rate) as "gms"
from shipment_gl_forecasts_all_v a
join fx_rates b
on a.country_code = b.source_country_code
join pids
using (portfolio_id)
where gl_product_group < 991
and gl_rollup not in ('dig','unk')
and target_day between '2012-01-01' and '2021-01-02'
group by 1,2,3,4,5,6,7

union all

select
'orders_final_forecast' as "initiative_name"
, 'orders (OSA)' as "type"
, to_char(dataset_date, 'YYYY-MM-DD') as "portfolio_id"
, country_code
, channel
, gl_rollup
, target_day
, sum(net_ordered_units) as "shu"
, sum(net_ops * fx_rate) as "prv"
, sum(net_ordered_gms * fx_rate) as "gms"
from toplinef_test.orders_gl_forecasts_daily a
join fx_rates b
on a.country_code = b.source_country_code
where forecast_id in (select forecast_id from (select dataset_date, max(forecast_id) as "forecast_id" from toplinef_test.orders_gl_forecasts_daily where forecast_type = 'Adjusted' group by 1))
and dataset_date >= '2019-06-01'
and gl_product_group < 991
and gl_rollup not in ('dig','unk')
and target_day between '2012-01-01' and '2021-01-02'
and forecast_type = 'Adjusted'
group by 1,2,3,4,5,6,7