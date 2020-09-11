/*+ ETLM {
    depend:{
        add:[
            { name:"TOPLINEF_DDL.shipment_gl_forecasts_all_v" },
        ]
    }
}*/
create temporary table fx_rate as 

  SELECT source_country_code AS "country_code",
         fx_rate,
         scenario
  FROM topline_fx_rates
  WHERE scenario = (select scenario from toplinef_test.tableau_fx_metadata_integ)

;

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

create temporary table max_actuals as 

    select max(target_day) as "target_day" from shipment_gl_actuals_v where extract(dow from target_day) = 6
;

create temporary table daily as

    SELECT 'Shipments' AS "type",
           portfolio_id,
           country_code,
           channel,
           subchannel,
           gl_rollup,
           gl_product_group || ' - ' || gl_name AS gl_product_group,
           prime_group,
           initiative,
           initiative_type,
           target_day AS "target_day",
           max(scenario) as "fx_scenario",
           SUM(CASE WHEN country_code = 'IN' THEN gross_gms_amt*fx_rate ELSE CASE WHEN channel = 'ret' THEN product_revenue_amt*fx_rate ELSE gross_gms_amt*fx_rate END END) AS "gms",
           SUM(shipped_units) AS "shu"
    FROM shipment_gl_forecasts_all_v
      JOIN fx_rate USING (country_code)
      JOIN pids USING (portfolio_id)
    WHERE gl_product_group < 991
    AND   gl_rollup NOT IN ('dig','unk')
    AND   target_day >= '2017-01-01'
    AND   forecast_id != 5299
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    
    UNION
    
    SELECT 'Shipments' AS "type",
           'Actuals' AS "portfolio_id",
           country_code,
           channel,
           subchannel,
           gl_rollup,
           gl_product_group || ' - ' || gl_name AS gl_product_group,
           prime_group,
           'Baseline' AS "initiative",
           'Baseline' AS "initiative_type",
           target_day AS "target_day",
           max(scenario) as "fx_scenario",
           SUM(CASE WHEN country_code = 'IN' THEN gross_gms_amt*fx_rate ELSE CASE WHEN channel = 'ret' THEN product_revenue_amt*fx_rate ELSE gross_gms_amt*fx_rate END END) AS "gms",
           SUM(shipped_units) AS "shu"
    FROM shipment_gl_actuals_v
      JOIN fx_rate USING (country_code)
    WHERE gl_product_group < 991
    AND   gl_rollup NOT IN ('dig','unk')
    AND   target_day >= '2017-01-01'
    AND   target_day <= (SELECT target_day FROM max_actuals)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    
    UNION
    
    SELECT 'orders (OSA final adjusted)' AS "type",
           p.portfolio_id,
           f.country_code,
           f.channel,
           f.channel AS subchannel,
           f.gl_rollup,
           f.gl_product_group || ' - ' || gl_name AS gl_product_group,
           f.prime_group,
           'orders_final_forecast' AS "initiative",
           'orders_final_forecast' AS "initiative_type",
           f.target_day,
           max(scenario) as "fx_scenario",
           SUM(CASE WHEN f.country_code = 'IN' THEN f.net_ordered_gms*fx_rate ELSE CASE WHEN f.channel = 'ret' THEN net_ops*fx_rate ELSE net_ordered_gms*fx_rate END END) AS "gms",
           SUM(net_ordered_units) AS "shu"
    FROM toplinef_test.orders_gl_forecasts_daily f
      JOIN forecast_portfolios p using (forecast_id)
      JOIN pids using (portfolio_id)
      JOIN fx_rate USING (country_code)
      JOIN topline_visible_gl_list v
        ON v.gl_product_group = f.gl_product_group
       AND v.visible_gl_product_group IS NOT NULL
    WHERE f.forecast_id IN (SELECT forecast_id
                            FROM (SELECT dataset_date,
                                         MAX(forecast_id) AS "forecast_id"
                                  FROM toplinef_test.orders_gl_forecasts_daily
                                  WHERE forecast_type = 'Adjusted'
                                  GROUP BY 1))
    AND   f.dataset_date >= '2019-06-01'
    AND   f.gl_product_group < 991
    AND   f.gl_rollup NOT IN ('dig','unk')
    AND   f.target_day >= '2017-01-01'
    AND   f.forecast_type = 'Adjusted'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    
    UNION
    
    SELECT 'orders (OSA final adjusted)' AS "type",
           'Actuals' AS "portfolio_id",
           f.country_code,
           f.channel,
           f.channel AS subchannel,
           f.gl_rollup,
           f.gl_product_group || ' - ' || gl_name AS gl_product_group,
           f.prime_group,
           'orders_final_forecast' AS "initiative",
           'orders_final_forecast' AS "initiative_type",
           f.order_day AS target_day,
           max(scenario) as "fx_scenario",
           SUM(CASE WHEN f.country_code = 'IN' THEN f.net_ordered_gms*fx_rate ELSE CASE WHEN f.channel = 'ret' THEN net_ops*fx_rate ELSE net_ordered_gms*fx_rate END END) AS "gms",
           SUM(net_ordered_units) AS "shu"
    FROM order_actuals_osa_v f
      JOIN fx_rate USING (country_code)
      JOIN topline_visible_gl_list v
        ON v.gl_product_group = f.gl_product_group
       AND v.visible_gl_product_group IS NOT NULL
    WHERE f.gl_product_group < 991
    AND   f.gl_rollup NOT IN ('dig','unk')
    AND   f.order_day >= '2017-01-01'
    AND   f.order_day <= (SELECT target_day FROM max_actuals)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    
;

create temporary table final_data as 

SELECT TYPE::VARCHAR(100) AS "type",
       'Weekly'::VARCHAR(100) AS "date_granularity",
       portfolio_id::VARCHAR(100),
       country_code::VARCHAR(100),
       channel::VARCHAR(100),
       subchannel::VARCHAR(100),
       gl_rollup::VARCHAR(100),
       gl_product_group::VARCHAR(100),
       prime_group::VARCHAR(100),
       initiative::VARCHAR(100),
       initiative_type::VARCHAR(100),
       DATE_TRUNC('week',target_day +1) -1 AS "target_day",
       max(fx_scenario) as "fx_scenario",
       SUM(gms) AS "gms",
       SUM(shu) AS "shu"
FROM daily
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
UNION ALL
SELECT TYPE::VARCHAR(100) AS "type",
       'Monthly'::VARCHAR(100) AS "date_granularity",
       portfolio_id::VARCHAR(100),
       country_code::VARCHAR(100),
       channel::VARCHAR(100),
       subchannel::VARCHAR(100),
       gl_rollup::VARCHAR(100),
       gl_product_group::VARCHAR(100),
       prime_group::VARCHAR(100),
       initiative::VARCHAR(100),
       initiative_type::VARCHAR(100),
       DATE_TRUNC('month',target_day) AS "target_day",
       max(fx_scenario) as "fx_scenario",
       SUM(gms) AS "gms",
       SUM(shu) AS "shu"
FROM daily
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
UNION ALL
SELECT TYPE::VARCHAR(100) AS "type",
       'Quarterly'::VARCHAR(100) AS "date_granularity",
       portfolio_id::VARCHAR(100),
       country_code::VARCHAR(100),
       channel::VARCHAR(100),
       subchannel::VARCHAR(100),
       gl_rollup::VARCHAR(100),
       gl_product_group::VARCHAR(100),
       prime_group::VARCHAR(100),
       initiative::VARCHAR(100),
       initiative_type::VARCHAR(100),
       DATE_TRUNC('quarter',target_day) AS "target_day",
       max(fx_scenario) as "fx_scenario",
       SUM(gms) AS "gms",
       SUM(shu) AS "shu"
FROM daily
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
UNION ALL
SELECT TYPE::VARCHAR(100) AS "type",
       'Yearly'::VARCHAR(100) AS "date_granularity",
       portfolio_id::VARCHAR(100),
       country_code::VARCHAR(100),
       channel::VARCHAR(100),
       subchannel::VARCHAR(100),
       gl_rollup::VARCHAR(100),
       gl_product_group::VARCHAR(100),
       prime_group::VARCHAR(100),
       initiative::VARCHAR(100),
       initiative_type::VARCHAR(100),
       DATE_TRUNC('year',target_day)::DATE AS "target_day",
       max(fx_scenario) as "fx_scenario",
       SUM(gms) AS "gms",
       SUM(shu) AS "shu"
FROM daily
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

;

delete from toplinef_test.phares_forecast_comparison_tableau_shipments_{FREE_FORM};

insert into toplinef_test.phares_forecast_comparison_tableau_shipments_{FREE_FORM}
select * from final_data
;

grant select on toplinef_test.phares_forecast_comparison_tableau_shipments_{FREE_FORM} to topline_tableau, toplinef_ro, bdt_rw, topline_bi;
commit;