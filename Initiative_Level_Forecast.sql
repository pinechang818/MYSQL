select --comment
portfolio_id
, country_code
, channel
, subchannel
, gl_rollup
, gl_product_group
, initiative_type
, initiative
, extract(month from target_day) as "target_month"
, extract(quarter from target_day) as "target_quarter"
, extract(year from target_day) as "target_year"
, sum(case when country_code = 'IN' then gross_gms_amt * fx_rate else case when channel = 'ret' then product_revenue_amt * fx_rate else gross_gms_amt * fx_rate end end) as "gms"
, sum(shipped_units) as "shu"
from shipment_gl_forecasts_all_v a
join topline_fx_rates b
on a.country_code = b.source_country_code
and b.scenario = '2019 OP2'
where dataset_date >= '2018-12-31'
and gl_product_group < 991
and gl_rollup not in ('dig','unk')
and extract(year from target_day) >= 2018
and forecast_id <> 5299
and initiative_type = 'Baseline'
group by 1,2,3,4,5,6,7,8,9,10,11