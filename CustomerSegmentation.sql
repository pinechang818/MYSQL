with pids as (
  select 
portfolio_id, 
rank() over (partition by portfolio_status order by creation_date desc) as "rank"
from toplinef_ddl.portfolio_metadata
where portfolio_status = 'PRODUCTION'
and created_by in ('root', 'toplinef')
and published = 'true'
order by creation_date desc
limit 5
)

--- To pull a portfolio into this dashboard, the portfolio ID must be manually specified.
select
portfolio_id
, scenario
, country_code
, customer_group
, plan_status
, plan_type
, tenure
, date_trunc('month',target_day::date) as target_month
, sum(shipped_units) as shipped_units
, sum(gms*fx_rate) as gms_usd
, sum(members-coalesce(twitch_shock,0))/avg(days_in_month) as avg_members

from (

select 
portfolio_id
, 'system' as scenario
, country_code
, customer_group
, plan_status
, plan_type
, tenure
, target_day::date as target_day
, sum(shipped_units) as shipped_units
, sum(case 
      when channel = 'ret' 
      then product_revenue_amt
      else gross_gms_amt 
      end) as gms
from toplinef_ddl.cs_ship_gl_tenure_final_vending join pids using (portfolio_id)
where cs_tag = 'Refresh'
and to_date(forecast_date,'YYYYMMDD') = to_date(substring(portfolio_id,1,8),'YYYYMMDD') -- forecasts only, no backtests
and initiative = 'baseline' -- system forecast
and country_code in ('US','CA','DE','UK','FR','IT','ES','JP')
and gl_rollup in ('hdl','sft','con','med')
and gl_product_group < 990
and extract('year' from target_day::date) >= 2019
group by 1,2,3,4,5,6,7,8

union all

select
portfolio_id
, 'final' as scenario
, country_code
, customer_group
, plan_status
, plan_type
, tenure
, target_day::date as target_day
, sum(shipped_units) as shipped_units
, sum(case 
      when channel = 'ret' 
      then product_revenue_amt
      else gross_gms_amt 
      end) as gms
from toplinef_ddl.cs_ship_gl_tenure_final_vending join pids using (portfolio_id)
where cs_tag = 'Refresh'
and to_date(forecast_date,'YYYYMMDD') = to_date(substring(portfolio_id,1,8),'YYYYMMDD') -- forecasts only, no backtests
and country_code in ('US','CA','DE','UK','FR','IT','ES','JP')
and gl_rollup in ('hdl','sft','con','med')
and gl_product_group < 990
and extract('year' from target_day::date) >= 2019
group by 1,2,3,4,5,6,7,8) as prv_gms

left join (

with max_portfolio as (
  select set_id,
         portfolio_id,
         portfolio_set_created_time,
         rank() over (
            partition by portfolio_id 
            order by portfolio_set_created_time desc
            ) as rank
  from tdc_prod.member_portfolio_sets
), 
member_run_ids as (
select 
       mp.portfolio_id,
       mp.set_id,
       mbs.forecast_date,
       mbs.scenario,
       mbs.run_id
from tdc_prod.member_sets mbs
join max_portfolio mp on (mbs.set_id = mp.set_id)
where mp.rank = 1
and to_date(substring(mp.portfolio_id,1,8),'YYYYMMDD') >= date('2019-01-01')
and (mp.portfolio_id like '%a' or mp.portfolio_id like '%b')
and (mp.portfolio_id not like '%pit%')
and (mp.portfolio_id not like '%bit%')
and mbs.scenario in ('system','final')
),
tenure_count as (
select
portfolio_id
, country_code
, customer_group
, plan_status
, plan_type
, count(distinct tenure) as tenure_count
from toplinef_ddl.cs_ship_gl_tenure_final_vending join pids using (portfolio_id)
where cs_tag = 'Refresh'
and to_date(forecast_date,'YYYYMMDD') = to_date(substring(portfolio_id,1,8),'YYYYMMDD') -- forecasts only, no backtests
and country_code in ('US','CA','DE','UK','FR','IT','ES','JP')
and channel = 'ret'
and subchannel = 'ret'
and gl_rollup = 'med'
and gl_product_group = 14
and target_day::date = to_date(forecast_date,'YYYYMMDD')
group by 1,2,3,4,5
)


select 
mri.portfolio_id
, mri.scenario
, mr.country_code
, mr.customer_group
, case when mr.customer_group = 'npr' then 'npr' else mr.plan_status end as plan_status
, mr.plan_type
, '0' as tenure
, mr.target_day::date as target_day
, extract('day' from last_day(mr.target_day::date)) as days_in_month
, sum(mr.member_count) as members
from tdc_prod.member_runs mr
join member_run_ids mri
using (run_id)
where run_id in (select run_id from member_run_ids)
and (mr.customer_group = 'npr' or mr.plan_status = 'free')
and extract('year' from mr.target_day::date) >= 2019
group by 1,2,3,4,5,6,7,8

union all

select 
mri.portfolio_id
, mri.scenario
, mcr.country_code
, mcr.customer_group
, mcr.plan_status
, mcr.plan_type
, case when tc.tenure_count = 1 then '0'
  else
    case when floor(months_between(mcr.target_day,mcr.cohort_month)) < 12 then '0'
         else '1'
    end
  end as tenure
, mcr.target_day::date as target_day
, extract('day' FROM last_day(target_day::date)) as days_in_month
, sum(mcr.member_count) as members
from tdc_prod.member_cohort_runs mcr
join member_run_ids mri
using (run_id)
join tenure_count tc
on (
mri.portfolio_id = tc.portfolio_id
and mcr.country_code = tc.country_code
and mcr.customer_group = tc.customer_group
and mcr.plan_status = tc.plan_status
and mcr.plan_type = tc.plan_type
)
where run_id in (select run_id from member_run_ids)
and mcr.plan_status = 'paid'
and extract('year' from mcr.target_day::date) >= 2019
group by 1,2,3,4,5,6,7,8) as mem

using (
portfolio_id
, scenario
, country_code
, customer_group
, plan_status
, plan_type
, tenure
, target_day
)

left join (

select 
country_code
, customer_group
, case when customer_group = 'npr' then 'npr' else plan_status end as plan_status
, plan_type
, '0' as tenure
, target_day::date as target_day
, sum(member_count) as twitch_shock
from tdc_test.member_twitch_shock_20190129
where country_code = 'US'
and target_day >= date('2018-01-01')
group by 1,2,3,4,5,6
) as ts

using (
country_code
, customer_group
, plan_status
, plan_type
, tenure
, target_day)

left join (

select 
source_country_code as country_code
, fx_rate
, updated_date
from toplinef_ddl.topline_fx_rates
where target_country_code = 'US'
and scenario = '2020 OP2'
and source_country_code in ('US','CA','UK','DE','FR','IT','ES','JP')
) as fx_rates

using (country_code)

group by 1,2,3,4,5,6,7,8