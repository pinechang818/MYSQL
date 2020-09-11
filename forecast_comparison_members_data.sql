/*+ ETLM {
    depend:{
        add:[
            { name:"toplinef_ddl.member_actuals_v" },
        ]
    }
}*/

create temporary table pids as

   select distinct portfolio_id as "portfolio_id" from 
  (
    select portfolio_id from toplinef_test.tableau_portfolio_metadata_integ
    union
    -- last 5 PITs
    select portfolio_id from (
        select portfolio_id, rank() over (order by dataset_date desc, portfolio_id desc) as "rank"
        from (
            select distinct portfolio_id, dataset_date from shipment_gl_forecasts_all_v where dataset_date >= '2019-06-01' and portfolio_status = 'TEST'
            and 'INTERNAL' = '{FREE_FORM}'
        )
    )
    where rank <= 5
    union
    -- last 5 production
    select portfolio_id from (
        select portfolio_id, rank() over (order by dataset_date desc, portfolio_id desc) as "rank"
        from (
            select distinct portfolio_id, dataset_date from shipment_gl_forecasts_v where dataset_date >= '2019-06-01' and is_latest_publish = 'Y')
    )
    where rank <= 5
    union
    -- last 1 draft
    select portfolio_id from (
        select portfolio_id, rank() over (order by dataset_date desc, portfolio_id desc) as "rank"
        from (
            select distinct portfolio_id, dataset_date from shipment_gl_forecasts_all_v where dataset_date >= '2019-06-01' and portfolio_status = 'DRAFT'
            and 'INTERNAL' = '{FREE_FORM}'
        )
    )
    where rank = 1
    ) 
;
create temporary table max_portfolio as 

  select set_id,
         portfolio_id,
         portfolio_set_created_time,
         rank() over (
            partition by portfolio_id 
            order by portfolio_set_created_time desc) as rank
  from tdc_prod.member_portfolio_sets
  join pids using (portfolio_id)
;

create temporary table member_run_ids as 

select 
       mp.portfolio_id,
       mp.set_id,
       mbs.forecast_date,
       mbs.scenario,
       mbs.run_id
from tdc_prod.member_sets mbs
join max_portfolio mp on (mbs.set_id = mp.set_id)
where mp.rank = 1
and date(mbs.forecast_date) = to_date(substring(mp.portfolio_id,1,8),'YYYYMMDD') -- forecasts only, no backtests
and mbs.scenario = 'system'

;

create temporary table max_actuals as  

select max(target_day) as "target_day" from toplinef_ddl.member_actuals_v where extract(dow from target_day) = 6

;

create temporary table twitch_shock as 

        SELECT 
        'Monthly' AS date_granularity
        , country_code
        , customer_group
        , DATE_TRUNC('month',DATE (target_day)) AS target_day
        , sum(member_count) AS twitch_shock
        FROM tdc_test.member_twitch_shock_20190129
        WHERE country_code = 'US'
        AND target_day >= date('2018-01-01')
        GROUP BY 1,2,3,4
        
        union

        SELECT 
        'Weekly' AS date_granularity
        , country_code
        , customer_group
        , DATE_TRUNC('week',DATE (target_day)+1)-1 AS target_day
        , sum(member_count) AS twitch_shock
        FROM tdc_test.member_twitch_shock_20190129
        WHERE country_code = 'US'
        AND target_day >= date('2018-01-01')
        GROUP BY 1,2,3,4
        
       union
       
        SELECT 
        'Quarterly' AS date_granularity
        , country_code
        , customer_group
        , DATE_TRUNC('quarter',DATE (target_day)) AS target_day
        , sum(member_count) AS twitch_shock
        FROM tdc_test.member_twitch_shock_20190129
        WHERE country_code = 'US'
        AND target_day >= date('2018-01-01')
        GROUP BY 1,2,3,4   
        
       union
       
        SELECT 
        'Yearly' AS date_granularity
        , country_code
        , customer_group
        , DATE_TRUNC('year',DATE (target_day)) AS target_day
        , sum(member_count) AS twitch_shock
        FROM tdc_test.member_twitch_shock_20190129
        WHERE country_code = 'US'
        AND target_day >= date('2018-01-01')
        GROUP BY 1,2,3,4  
;

create temporary table base_data as 

    select
    mri.portfolio_id,
    mr.country_code,
    mr.customer_group,
    target_day as target_day,
    sum(mr.member_count) as member_count
    from tdc_prod.member_runs mr
    join member_run_ids mri
    using (run_id)
    where mr.target_day >= '2017-01-01'
    and mr.country_code not in ('MX')
    and mr.customer_group <> 'npi'
    group by 1,2,3,4
    
    union
    
    select 
    'Actuals' as portfolio_id,
    country_code,
    customer_group,
    target_day,
    sum(member_count) as member_count
    from toplinef_ddl.member_actuals_v
    where target_day between '2017-01-01' and (select target_day from max_actuals)
    and country_code <> 'MX'
    group by 1,2,3,4  

;

create temporary table date_granularities as 

    select
    'Monthly' as date_granularity,
    portfolio_id,
    country_code,
    customer_group,
    date_trunc('month', date(target_day)) as target_day,
    count(distinct target_day) as days_in_period,
    sum(member_count) as members
    from base_data
    group by 1,2,3,4,5

    union

    select
    'Weekly' as date_granularity,
    portfolio_id,
    country_code,
    customer_group,
    date_trunc('week', date(target_day)+1)-1 as target_day,
    count(distinct target_day) as days_in_period,
    sum(member_count) as members
    from base_data
    group by 1,2,3,4,5

    union

    select
    'Quarterly' as date_granularity,
    portfolio_id,
    country_code,
    customer_group,
    date_trunc('quarter', date(target_day)) as target_day,
    count(distinct target_day) as days_in_period,
    sum(member_count) as members
    from base_data
    group by 1,2,3,4,5

    union

    select
    'Yearly' as date_granularity,
    portfolio_id,
    country_code,
    customer_group,
    date_trunc('year', date(target_day)) as target_day,
    count(distinct target_day) as days_in_period,
    sum(member_count) as members
    from base_data
    group by 1,2,3,4,5

;

create temporary table final_data as
select b.date_granularity,
b.portfolio_id,
b.country_code,
b.customer_group as prime_group,
b.target_day,
sum(b.members-coalesce(twitch_shock,0)) / avg(days_in_period) as avg_members
from date_granularities b
left join twitch_shock
using (date_granularity, country_code, customer_group, target_day)
group by 1,2,3,4,5
;

delete from toplinef_test.phares_forecast_comparison_tableau_members_{FREE_FORM};

insert into toplinef_test.phares_forecast_comparison_tableau_members_{FREE_FORM}
select * from final_data
;

grant select on toplinef_test.phares_forecast_comparison_tableau_members_{FREE_FORM} to topline_tableau, toplinef_ro, bdt_rw;
commit;
