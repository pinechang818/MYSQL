with gl_0 as (
  select * 
  from toplinef_test.accheria_COVID_gl_promise_extension
  where gl_product_group = 0),
country_max as(
  select country_code,max(order_day) as max_order_day 
  from order_actuals_osa_v_q4_test
  group by 1),
region_country_max as (
  select 
    case when country_code in ('US','CA') then 'NA' 
    else 'EU' end as country_code,
    min(max_order_day) as max_order_day
  from country_max
  where country_code in ('US','CA','UK','DE','FR','IT','ES')
  group by 1
  union
  select * from country_max),
act_cy as (
  select a.country_code 
  , channel
  , prime_group
  , case 
      when cv.promise_extension is not null then cv.promise_extension
      else gl0.promise_extension
    end as GL_Custom_group
  , coalesce(glB.boss,'No') as GL_BOSS_group
  , prime_group
  , order_day as target_day
  , left(to_char(order_day + interval '1 day', 'IYYY-IW'),4)::INT as year
  , right(to_char(order_day + interval '1 day', 'IYYY-IW'),2) as week
  , extract(dow from order_day) as dow
  , sum(net_ordered_units) as act_units_cy
  , sum(case when channel = 'ret' then net_ops * fx_rate else net_ordered_gms * fx_rate end) as act_gms_cy
  from order_actuals_osa_v_q4_test a
  left join toplinef_test.accheria_COVID_gl_promise_extension cv
  on a.country_code = cv.country_code 
  and a.gl_product_group = cv.gl_product_group
  and a.category_code = coalesce(cv.category_code,a.category_code)
  left join gl_0 gl0 on a.country_code = gl0.country_code
  left join toplinef_test.accheria_COVID_FR_IT_BOSSing glB
  on a.country_code = glB.country_code 
  and a.gl_product_group = glB.gl_product_group
  and a.category_code = glB.category_code
  join topline_fx_rates b
  on a.country_code = b.source_country_code
  and b.scenario = '2019 OP2'
  where gl_rollup not in ('dig','unk')
  and a.gl_product_group < 991
  and a.country_code in ('CA','UK','DE','FR','IT','ES','US')
  and target_day >= '2019-12-29'
  group by 1,2,3,4,5,6,7,8,9,
),
act_py as (
    select a.country_code 
  , channel
  , prime_group
  , case 
      when cv.promise_extension is not null then cv.promise_extension
      else gl0.promise_extension
    end as GL_Custom_group
  , coalesce(glB.boss,'No') as GL_BOSS_group
  , prime_group
  , (left(to_char(order_day + interval '1 day', 'IYYY-IW'),4)::INT + 1)  as year
  , right(to_char(order_day + interval '1 day', 'IYYY-IW'),2) as week
  , extract(dow from order_day) as dow
  , sum(net_ordered_units) as act_units_py
  , sum(case when channel = 'ret' then net_ops * fx_rate else net_ordered_gms * fx_rate end) as act_gms_py
  from order_actuals_osa_v_q4_test a
  left join toplinef_test.accheria_COVID_gl_promise_extension cv
  on a.country_code = cv.country_code 
  and a.gl_product_group = cv.gl_product_group
  and a.category_code = coalesce(cv.category_code,a.category_code)
  left join gl_0 gl0 on a.country_code = gl0.country_code
  left join toplinef_test.accheria_COVID_FR_IT_BOSSing glB
  on a.country_code = glB.country_code 
  and a.gl_product_group = glB.gl_product_group
  and a.category_code = glB.category_code
  join topline_fx_rates b
  on a.country_code = b.source_country_code
  and b.scenario = '2019 OP2'
  where gl_rollup not in ('dig','unk')
  and a.gl_product_group < 991
  and a.country_code in ('CA','UK','DE','FR','IT','ES','US')
  and order_day between '2018-12-30' and '2019-06-30'
  group by 1,2,3,4,5,6,7,8,9
  
)
select c.*,act_units_py,act_gms_py,case when max_order_day - target_day < 7 then 1 else 0 end as T7
from act_cy c
join region_country_max using (country_code)
left join act_py p using (country_code,channel,GL_Custom_group,GL_BOSS_group,prime_group,year,week,dow)
where target_day <= max_order_day