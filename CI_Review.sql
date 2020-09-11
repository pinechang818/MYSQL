with latest_portfolios as (
select 
portfolio_id, 
dataset_date,
rank() over (partition by portfolio_status order by creation_date desc) as "rank"
from toplinef_ddl.portfolio_metadata
where portfolio_status = 'PRODUCTION'
and created_by in ('root', 'toplinef')
and published = 'true'
order by creation_date desc
limit 3
),

ci_fids as (
select 
lp.portfolio_id,
fm.dataset_date,
fm.forecast_id,
forecast_status,
forecast_description,
(case when forecast_description LIKE '%[IN,INT,WW]%' then 'IN CI'
when forecast_description like '%[CA,DE,ES,EU,FR,INT,IT,JP,NA,UK,US,WW]%' then '8 countries CI'
else 'other countries CI'
end) as ci_type,
"rank"
from toplinef_ddl.forecast_metadata fm
join toplinef_ddl.forecast_portfolios fp
on fm.forecast_id = fp.forecast_id
join latest_portfolios lp
on fm.dataset_date = lp.dataset_date
and fp.portfolio_id = lp.portfolio_id
where forecast_type = 'confidence_intervals'
and ((forecast_status in ('PRODUCTION', 'DRAFT') and "rank" = 1) or (forecast_status = 'PRODUCTION' and "rank" >1))
)


  select
  "rank",
 portfolio_id,
 ci_type,
 tci.forecast_id,
 forecast_date,
 TO_DATE(range_start,'YYYY-MM-DD') AS range_start,
 TO_DATE(range_end,'YYYY-MM-DD') AS range_end,
 grain,
 left(country_code, 3) as country_code,
 left(channel,3) as channel,
 left(gl_rollup_code,3) as gl_rollup_code,
 quantile,
 shipped_units,
 shipped_units_percentile_value,
 gms_prv,
 gms_prv_percentile_value
 
  FROM toplinef_ddl.topline_confidence_intervals tci
  join ci_fids cf
  on cf.forecast_id = tci.forecast_id
  and cf.dataset_date = tci.forecast_date
  WHERE gl_rollup_code IN ('all','con','sft','hdl','med')
  and ((ci_type = '8 countries CI') or (ci_type = 'IN CI' and country_code = 'IN'))
  order by 1,2,3,4,5,6,7,8,9