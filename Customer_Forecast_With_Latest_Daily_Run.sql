select
country_code
, customer_group
, plan_status
, plan_type
, target_day
, portfolio_id
, forecast_date
, scenario
, actuals_range
, cancel_count
, convert_in_count
, convert_out_count
, member_count
, start_count
, net_add_count
, cancel_rate
, convert_in_rate
, convert_out_rate
, start_rate

from (

with actuals_date_range as (
  select 
  country_code
  , max(snapshot_day) as max_actuals_day
  from toplinef_ddl.member_actuals_v
  group by country_code
)
, max_portfolio as (
  select set_id,
         portfolio_id,
         portfolio_set_created_time,
         rank() over (
            partition by portfolio_id 
            order by portfolio_set_created_time desc) as rank
  from tdc_prod.member_portfolio_sets
) 
, max_snapshot AS (
  select set_id,
         snapshot_day,
         snapshot_day_set_created_time,
         RANK() OVER (
            PARTITION BY snapshot_day 
            order by snapshot_day_set_created_time desc) as rank
  from tdc_prod.member_snapshot_day_sets
  where snapshot_day in ('2019-12-10') --list of daily run preview to share with prime finance team
)
, max_snapshot_daily AS (
  select set_id,
         snapshot_day,
         snapshot_day_set_created_time,
         RANK() OVER (
            PARTITION BY snapshot_day 
            order by snapshot_day_set_created_time desc) as rank
  from tdc_prod.member_snapshot_day_sets
 --list of daily run preview to share with prime finance team
 )
, member_run_ids as (
	-- published portfolios
select 
       mp.portfolio_id,
       mp.set_id,
       mbs.forecast_date,
       mbs.scenario,
       mbs.run_id
from tdc_prod.member_sets mbs
join max_portfolio mp on (mbs.set_id = mp.set_id)
where mp.rank = 1
and to_date(substring(mp.portfolio_id,1,8),'YYYYMMDD') >= date('2018-12-31')
and (mp.portfolio_id like '%a' or mp.portfolio_id like '%b')
and date(mbs.forecast_date) = to_date(substring(mp.portfolio_id,1,8),'YYYYMMDD') -- forecasts only, no backtests
and (mp.portfolio_id not like '%pit%')
and (mp.portfolio_id not like '%bit%')
and mbs.scenario in ('system','final')

union

	-- latest PIT
select 
       mp.portfolio_id,
       mp.set_id,
       mbs.forecast_date,
       mbs.scenario,
       mbs.run_id
from tdc_prod.member_sets mbs
join max_portfolio mp on (mbs.set_id = mp.set_id)
where mp.rank = 1
and mp.portfolio_id = (select max(portfolio_id) from max_portfolio)
and mp.portfolio_id like '%pit%'
and date(mbs.forecast_date) = to_date(substring(mp.portfolio_id,1,8),'YYYYMMDD') -- forecasts only, no backtests
and mbs.scenario in ('system','final')

union

	-- daily run for 12/10
select 
       split_part(ms.snapshot_day,'-',1) || split_part(ms.snapshot_day,'-',2) || split_part(ms.snapshot_day,'-',3) || '' as portfolio_id,
       ms.set_id,
       mbs.forecast_date,
       mbs.scenario,
       mbs.run_id
from tdc_prod.member_sets mbs
join max_snapshot ms on (mbs.set_id = ms.set_id)
where ms.rank = 1
and ms.snapshot_day = (select max(snapshot_day) from max_snapshot)
and mbs.forecast_date = ms.snapshot_day -- forecasts only, no backtests
and mbs.scenario in ('system','final')

union

select 
       split_part(msd.snapshot_day,'-',1) || split_part(msd.snapshot_day,'-',2) || split_part(msd.snapshot_day,'-',3) || ' Daily Run' as portfolio_id,
       msd.set_id,
       mbs.forecast_date,
       mbs.scenario,
       mbs.run_id
from tdc_prod.member_sets mbs
join max_snapshot_daily msd on (mbs.set_id = msd.set_id)
where msd.rank = 1
and msd.snapshot_day = (select max(snapshot_day) from max_snapshot_daily)
and mbs.forecast_date = msd.snapshot_day -- forecasts only, no backtests
and mbs.scenario in ('system','final')

 union

--  -- for viewing an unpublished forecast given a set_id
 select
 '20200224 preview' AS portfolio_id
 , set_id
 , forecast_date
 , scenario
 , run_id
 from tdc_prod.member_sets
 where set_id = '20200225204730_ffd625b1'
 and forecast_date = (select max(forecast_date) from tdc_prod.member_sets where set_id = '20200225204730_ffd625b1') -- forecast only, no backtests
 and scenario in ('system','final')

 union

  select
 '20200229 preview' AS portfolio_id
 , set_id
 , forecast_date
 , scenario
 , run_id
 from tdc_prod.member_sets
 where set_id = '20200302014540_bb7049e0'
 and forecast_date = (select max(forecast_date) from tdc_prod.member_sets where set_id = '20200302014540_bb7049e0') -- forecast only, no backtests
 and scenario in ('system','final')

 union

  select
 '20200229 preview 2' AS portfolio_id
 , set_id
 , forecast_date
 , scenario
 , run_id
 from tdc_prod.member_sets
 where set_id = '20200320162710_73b6db78'
 and forecast_date = (select max(forecast_date) from tdc_prod.member_sets where set_id = '20200302014540_bb7049e0') -- forecast only, no backtests
 and scenario in ('system','final')

 union

  select
 '20200229 preview 3' AS portfolio_id
 , set_id
 , forecast_date
 , scenario
 , run_id
 from tdc_prod.member_sets
 where set_id = '20200321064227_6592452e'
 and forecast_date = (select max(forecast_date) from tdc_prod.member_sets where set_id = '20200321064227_6592452e') -- forecast only, no backtests
 and scenario in ('system','final')
 union

  select
 '20200229 preview 4' AS portfolio_id
 , set_id
 , forecast_date
 , scenario
 , run_id
 from tdc_prod.member_sets
 where set_id = '20200324175125_0fcf1017'
 and forecast_date = (select max(forecast_date) from tdc_prod.member_sets where set_id = '20200324175125_0fcf1017') -- forecast only, no backtests
 and scenario in ('system','final')

-- union

--  -- for viewing an unpublished forecast given a bunch of run_ids
-- select
-- '' AS portfolio_id
-- , '' as set_id
-- , null as forecast_date
-- , 'system' as scenario
-- , run_id
-- from tdc_prod.member_sets
-- where run_id in ()
)
, rates as (
	select 
	mr.country_code
	, mr.customer_group
	, mr.plan_status
	, mr.plan_type
	, mr.target_day
	, mri.portfolio_id
	, mri.forecast_date
	, mri.scenario
	, mr.cancel_rate
	, mr.convert_in_rate
	, mr.convert_out_rate
	, mr.start_rate
	from tdc_prod.customer_rates_forecasts mr
	join member_run_ids mri
	using (run_id)
	where run_id in (select 
	                    run_id 
	                 from member_run_ids)
	and mr.target_day >= '2015-01-01'
	and mr.country_code not in ('MX')
)
(
	select 
	mr.country_code
	, mr.customer_group
	, mr.plan_status
	, mr.plan_type
	, mr.target_day
	, mri.portfolio_id
	, mri.forecast_date
	, mri.scenario
	, case when mr.target_day <= max_actuals_day then 1 else 0 end as actuals_range
	, mr.cancel_count
	, mr.convert_in_count
	, mr.convert_out_count
	, mr.member_count
	, mr.start_count
	, mr.start_count+mr.convert_in_count-mr.cancel_count-mr.convert_out_count as net_add_count
	, r.cancel_rate
	, r.convert_in_rate
	, r.convert_out_rate
	, r.start_rate
	from tdc_prod.member_runs mr
	join member_run_ids mri
	using (run_id)
	left join rates r 
	on (
		mr.country_code = r.country_code
		and mr.customer_group = r.customer_group
		and mr.plan_status = r.plan_status
		and mr.plan_type = r.plan_type
		and mr.target_day = r.target_day
		and mri.portfolio_id = r.portfolio_id
		and mri.scenario = r.scenario)
	join actuals_date_range ar
	on mr.country_code = ar.country_code
	where run_id in (select 
	                    run_id 
	                 from member_run_ids)
	and mr.target_day >= '2015-01-01'
	and mr.country_code not in ('MX')
)

union

(
	select 
	country_code
	, customer_group
	, plan_status
	, plan_type
	, target_day
	, 'Actuals' as portfolio_id
	, NULL as forecast_date
	, 'Actuals' as scenario
	, 1 as actuals_range
	, cancel_count
	, convert_in_count
	, convert_out_count
	, member_count
	, start_count
	, start_count+convert_in_count-cancel_count-convert_out_count as net_add_count
	, NULL as cancel_rate
	, NULL as convert_in_rate
	, NULL as convert_out_rate
	, NULL as start_rate
	from toplinef_ddl.member_actuals_v
	where target_day >= '2015-01-01'
	and country_code not in ('MX')
)

)