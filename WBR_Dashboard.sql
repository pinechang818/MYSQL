DROP VIEW IF EXISTS toplinef_test.tableau_wbr_dashboard_v CASCADE;

CREATE OR REPLACE VIEW toplinef_test.tableau_wbr_dashboard_v
(
  ship_order,
  latest_act,
  region,
  country_code,
  channel,
  gl_rollup,
  gl_product_group,
  gl_name,
  prime_group,
  target_day,
  portfolio_id,
  initiative,
  units_fcst,
  rev_fcst,
  units_act,
  rev_act
)
AS 
 SELECT d.ship_order, l.latest_act, d."region", d.country_code, d.channel, d.gl_rollup, d.gl_product_group, d.gl_name, d.prime_group, d.target_day, d.portfolio_id, d.initiative, d.units_fcst, d.rev_fcst * fx.fx_rate AS rev_fcst, d.units_act, d.rev_act * fx.fx_rate AS rev_act
   FROM ( SELECT 'Shipments' AS ship_order, 
                CASE
                    WHEN COALESCE(sf.country_code, sa.country_code) = 'US'::bpchar OR COALESCE(sf.country_code, sa.country_code) = 'CA'::bpchar THEN 'NA'::text
                    WHEN COALESCE(sf.country_code, sa.country_code) = 'UK'::bpchar OR COALESCE(sf.country_code, sa.country_code) = 'DE'::bpchar OR COALESCE(sf.country_code, sa.country_code) = 'FR'::bpchar OR COALESCE(sf.country_code, sa.country_code) = 'IT'::bpchar OR COALESCE(sf.country_code, sa.country_code) = 'ES'::bpchar THEN 'EU'::text
                    WHEN COALESCE(sf.country_code, sa.country_code) = 'JP'::bpchar THEN 'JP'::text
                    WHEN COALESCE(sf.country_code, sa.country_code) = 'IN'::bpchar THEN 'IN'::text
                    ELSE NULL::text
                END::character varying AS "region", COALESCE(sf.country_code, sa.country_code) AS country_code, COALESCE(sf.channel, sa.channel) AS channel, COALESCE(sf.gl_rollup, sa.gl_rollup) AS gl_rollup, COALESCE(sf.gl_product_group, sa.gl_product_group) AS gl_product_group, COALESCE(sf.gl_name, sa.gl_name) AS gl_name, COALESCE(sf.prime_group, sa.prime_group::character varying) AS prime_group, COALESCE(sf.target_day, sa.target_day) AS target_day, sf.portfolio_id, sf.initiative::character varying AS initiative, sf.units_fcst, sf.rev_fcst, sa.units_act, sa.rev_act
           FROM ( SELECT shipment_gl_forecasts_v.portfolio_id, 'Final'::text AS initiative, shipment_gl_forecasts_v.country_code, shipment_gl_forecasts_v.channel, shipment_gl_forecasts_v.gl_rollup, shipment_gl_forecasts_v.gl_product_group, shipment_gl_forecasts_v.gl_name, 
                        CASE
                            WHEN shipment_gl_forecasts_v.prime_group::text = 'prm'::text OR shipment_gl_forecasts_v.prime_group::text = 'stu'::text THEN 'prm'::text
                            ELSE 'npr'::text
                        END::character varying AS prime_group, shipment_gl_forecasts_v.target_day, sum(shipment_gl_forecasts_v.shipped_units) AS units_fcst, sum(
                        CASE
                            WHEN shipment_gl_forecasts_v.channel::text = 'ret'::text THEN shipment_gl_forecasts_v.product_revenue_amt
                            ELSE shipment_gl_forecasts_v.gross_gms_amt
                        END) AS rev_fcst, shipment_gl_forecasts_v.dataset_date
                   FROM shipment_gl_forecasts_v
              JOIN ( SELECT prod_portfolios.portfolio_id, prod_portfolios.dataset_date, pg_catalog.rank()
                          OVER( 
                          ORDER BY prod_portfolios.dataset_date DESC) AS portfolio_rank
                           FROM ( SELECT DISTINCT shipment_gl_forecasts_v.portfolio_id, shipment_gl_forecasts_v.dataset_date
                                   FROM shipment_gl_forecasts_v
                                  WHERE shipment_gl_forecasts_v.portfolio_status::text = 'PRODUCTION'::text AND shipment_gl_forecasts_v.is_latest_publish::text = 'Y'::text) prod_portfolios) ranked_portfolios USING (portfolio_id, dataset_date)
             WHERE shipment_gl_forecasts_v.target_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND shipment_gl_forecasts_v.target_day <= date_add('month'::text, 13::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND shipment_gl_forecasts_v.gl_product_group < 990 AND shipment_gl_forecasts_v.gl_rollup <> 'dig'::bpchar AND shipment_gl_forecasts_v.gl_rollup <> 'unk'::bpchar AND (shipment_gl_forecasts_v.country_code = 'US'::bpchar OR shipment_gl_forecasts_v.country_code = 'CA'::bpchar OR shipment_gl_forecasts_v.country_code = 'UK'::bpchar OR shipment_gl_forecasts_v.country_code = 'DE'::bpchar OR shipment_gl_forecasts_v.country_code = 'FR'::bpchar OR shipment_gl_forecasts_v.country_code = 'IT'::bpchar OR shipment_gl_forecasts_v.country_code = 'ES'::bpchar OR shipment_gl_forecasts_v.country_code = 'JP'::bpchar OR shipment_gl_forecasts_v.country_code = 'IN'::bpchar) AND (ranked_portfolios.portfolio_rank <= 5 OR shipment_gl_forecasts_v.portfolio_id::text = '20191229a'::text)
             GROUP BY shipment_gl_forecasts_v.portfolio_id, 2, shipment_gl_forecasts_v.country_code, shipment_gl_forecasts_v.channel, shipment_gl_forecasts_v.gl_rollup, shipment_gl_forecasts_v.gl_product_group, shipment_gl_forecasts_v.gl_name, 
                   CASE
                       WHEN shipment_gl_forecasts_v.prime_group::text = 'prm'::text OR shipment_gl_forecasts_v.prime_group::text = 'stu'::text THEN 'prm'::text
                       ELSE 'npr'::text
                   END::character varying, shipment_gl_forecasts_v.target_day, shipment_gl_forecasts_v.dataset_date
        UNION 
                 SELECT shipment_gl_forecasts_v.portfolio_id, 'System'::text AS initiative, shipment_gl_forecasts_v.country_code, shipment_gl_forecasts_v.channel, shipment_gl_forecasts_v.gl_rollup, shipment_gl_forecasts_v.gl_product_group, shipment_gl_forecasts_v.gl_name, 
                        CASE
                            WHEN shipment_gl_forecasts_v.prime_group::text = 'prm'::text OR shipment_gl_forecasts_v.prime_group::text = 'stu'::text THEN 'prm'::text
                            ELSE 'npr'::text
                        END::character varying AS prime_group, shipment_gl_forecasts_v.target_day, sum(shipment_gl_forecasts_v.shipped_units) AS units_fcst, sum(
                        CASE
                            WHEN shipment_gl_forecasts_v.channel::text = 'ret'::text THEN shipment_gl_forecasts_v.product_revenue_amt
                            ELSE shipment_gl_forecasts_v.gross_gms_amt
                        END) AS rev_fcst, shipment_gl_forecasts_v.dataset_date
                   FROM shipment_gl_forecasts_v
              JOIN ( SELECT prod_portfolios.portfolio_id, prod_portfolios.dataset_date, pg_catalog.rank()
                          OVER( 
                          ORDER BY prod_portfolios.dataset_date DESC) AS portfolio_rank
                           FROM ( SELECT DISTINCT shipment_gl_forecasts_v.portfolio_id, shipment_gl_forecasts_v.dataset_date
                                   FROM shipment_gl_forecasts_v
                                  WHERE shipment_gl_forecasts_v.portfolio_status::text = 'PRODUCTION'::text AND shipment_gl_forecasts_v.is_latest_publish::text = 'Y'::text) prod_portfolios) ranked_portfolios USING (portfolio_id, dataset_date)
             WHERE shipment_gl_forecasts_v.target_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND shipment_gl_forecasts_v.target_day <= date_add('month'::text, 13::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND shipment_gl_forecasts_v.gl_product_group < 990 AND shipment_gl_forecasts_v.gl_rollup <> 'dig'::bpchar AND shipment_gl_forecasts_v.gl_rollup <> 'unk'::bpchar AND shipment_gl_forecasts_v.initiative_type::text = 'Baseline'::text AND (shipment_gl_forecasts_v.country_code = 'US'::bpchar OR shipment_gl_forecasts_v.country_code = 'CA'::bpchar OR shipment_gl_forecasts_v.country_code = 'UK'::bpchar OR shipment_gl_forecasts_v.country_code = 'DE'::bpchar OR shipment_gl_forecasts_v.country_code = 'FR'::bpchar OR shipment_gl_forecasts_v.country_code = 'IT'::bpchar OR shipment_gl_forecasts_v.country_code = 'ES'::bpchar OR shipment_gl_forecasts_v.country_code = 'JP'::bpchar OR shipment_gl_forecasts_v.country_code = 'IN'::bpchar) AND (ranked_portfolios.portfolio_rank <= 5 OR shipment_gl_forecasts_v.portfolio_id::text = '20191229a'::text)
             GROUP BY shipment_gl_forecasts_v.portfolio_id, 2, shipment_gl_forecasts_v.country_code, shipment_gl_forecasts_v.channel, shipment_gl_forecasts_v.gl_rollup, shipment_gl_forecasts_v.gl_product_group, shipment_gl_forecasts_v.gl_name, 
                   CASE
                       WHEN shipment_gl_forecasts_v.prime_group::text = 'prm'::text OR shipment_gl_forecasts_v.prime_group::text = 'stu'::text THEN 'prm'::text
                       ELSE 'npr'::text
                   END::character varying, shipment_gl_forecasts_v.target_day, shipment_gl_forecasts_v.dataset_date) sf
      FULL JOIN ( SELECT shipment_gl_actuals_v.country_code, shipment_gl_actuals_v.channel, shipment_gl_actuals_v.gl_rollup, shipment_gl_actuals_v.gl_product_group, shipment_gl_actuals_v.gl_name, 
                        CASE
                            WHEN shipment_gl_actuals_v.prime_group::text = 'prm'::text OR shipment_gl_actuals_v.prime_group::text = 'stu'::text THEN 'prm'::text
                            ELSE 'npr'::text
                        END AS prime_group, shipment_gl_actuals_v.target_day, sum(shipment_gl_actuals_v.shipped_units) AS units_act, sum(
                        CASE
                            WHEN shipment_gl_actuals_v.channel::text = 'ret'::text THEN shipment_gl_actuals_v.product_revenue_amt
                            ELSE shipment_gl_actuals_v.gross_gms_amt
                        END) AS rev_act
                   FROM shipment_gl_actuals_v
                  WHERE shipment_gl_actuals_v.target_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND shipment_gl_actuals_v.gl_product_group < 990 AND shipment_gl_actuals_v.gl_rollup <> 'dig'::bpchar AND shipment_gl_actuals_v.gl_rollup <> 'unk'::bpchar AND (shipment_gl_actuals_v.country_code = 'US'::bpchar OR shipment_gl_actuals_v.country_code = 'CA'::bpchar OR shipment_gl_actuals_v.country_code = 'UK'::bpchar OR shipment_gl_actuals_v.country_code = 'DE'::bpchar OR shipment_gl_actuals_v.country_code = 'FR'::bpchar OR shipment_gl_actuals_v.country_code = 'IT'::bpchar OR shipment_gl_actuals_v.country_code = 'ES'::bpchar OR shipment_gl_actuals_v.country_code = 'JP'::bpchar OR shipment_gl_actuals_v.country_code = 'IN'::bpchar)
                  GROUP BY shipment_gl_actuals_v.country_code, shipment_gl_actuals_v.channel, shipment_gl_actuals_v.gl_rollup, shipment_gl_actuals_v.gl_product_group, shipment_gl_actuals_v.gl_name, 
                        CASE
                            WHEN shipment_gl_actuals_v.prime_group::text = 'prm'::text OR shipment_gl_actuals_v.prime_group::text = 'stu'::text THEN 'prm'::text
                            ELSE 'npr'::text
                        END, shipment_gl_actuals_v.target_day) sa USING (country_code, channel, gl_rollup, gl_product_group, gl_name, prime_group, target_day)
UNION 
         SELECT 'Orders' AS ship_order, 
                CASE
                    WHEN COALESCE("of".country_code, oa.country_code::character varying)::text = 'US'::text OR COALESCE("of".country_code, oa.country_code::character varying)::text = 'CA'::text THEN 'NA'::text
                    WHEN COALESCE("of".country_code, oa.country_code::character varying)::text = 'UK'::text OR COALESCE("of".country_code, oa.country_code::character varying)::text = 'DE'::text OR COALESCE("of".country_code, oa.country_code::character varying)::text = 'FR'::text OR COALESCE("of".country_code, oa.country_code::character varying)::text = 'IT'::text OR COALESCE("of".country_code, oa.country_code::character varying)::text = 'ES'::text THEN 'EU'::text
                    WHEN COALESCE("of".country_code, oa.country_code::character varying)::text = 'JP'::text THEN 'JP'::text
                    WHEN COALESCE("of".country_code, oa.country_code::character varying)::text = 'IN'::text THEN 'IN'::text
                    ELSE NULL::text
                END::character varying AS "region", COALESCE("of".country_code, oa.country_code::character varying) AS country_code, COALESCE("of".channel, oa.channel) AS channel, COALESCE("of".gl_rollup, oa.gl_rollup::character varying) AS gl_rollup, COALESCE("of".gl_product_group, oa.gl_product_group) AS gl_product_group, COALESCE("of".gl_name, oa.gl_name::character varying) AS gl_name, COALESCE("of".prime_group, oa.prime_group::character varying) AS prime_group, COALESCE("of".target_day, oa.target_day) AS target_day, "of".portfolio_id, "of".initiative::character varying AS initiative, "of".units_fcst, "of".rev_fcst, oa.units_act, oa.rev_act
           FROM ( SELECT ranked_forecast_id.portfolio_id, 'Final'::text AS initiative, adjusted_orders_forecast.country_code, adjusted_orders_forecast.channel, adjusted_orders_forecast.gl_rollup, adjusted_orders_forecast.gl_product_group, gl_list.gl_name::character varying AS gl_name, 
                        CASE
                            WHEN adjusted_orders_forecast.prime_group::text = 'prm'::text OR adjusted_orders_forecast.prime_group::text = 'stu'::text THEN 'prm'::text
                            ELSE 'npr'::text
                        END::character varying AS prime_group, adjusted_orders_forecast.target_day, sum(adjusted_orders_forecast.net_ordered_units) AS units_fcst, sum(adjusted_orders_forecast.net_ordered_gms) AS rev_fcst, adjusted_orders_forecast.dataset_date
                   FROM adjusted_orders_forecast
              JOIN ( SELECT order_forecast_ids.forecast_type, ranked_portfolios.portfolio_id, order_forecast_ids.dataset_date, order_forecast_ids.forecast_id, pg_catalog.rank()
                          OVER( 
                          PARTITION BY order_forecast_ids.forecast_type
                          ORDER BY order_forecast_ids.dataset_date DESC) AS forecast_rank
                           FROM ( SELECT DISTINCT 'Baseline'::text AS forecast_type, orders_gl_forecasts_daily.dataset_date, "max"(orders_gl_forecasts_daily.forecast_id) AS forecast_id
                                   FROM orders_gl_forecasts_daily
                                  GROUP BY 1, orders_gl_forecasts_daily.dataset_date
                        UNION 
                                 SELECT DISTINCT 'Adjusted'::text AS forecast_type, adjusted_orders_forecast.dataset_date, "max"(adjusted_orders_forecast.forecast_id) AS forecast_id
                                   FROM adjusted_orders_forecast
                                  GROUP BY 1, adjusted_orders_forecast.dataset_date) order_forecast_ids
                      JOIN ( SELECT prod_portfolios.portfolio_id, prod_portfolios.dataset_date, pg_catalog.rank()
                                  OVER( 
                                  ORDER BY prod_portfolios.dataset_date DESC) AS portfolio_rank
                                   FROM ( SELECT DISTINCT shipment_gl_forecasts_v.portfolio_id, shipment_gl_forecasts_v.dataset_date
                                           FROM shipment_gl_forecasts_v
                                          WHERE shipment_gl_forecasts_v.portfolio_status::text = 'PRODUCTION'::text AND shipment_gl_forecasts_v.is_latest_publish::text = 'Y'::text) prod_portfolios) ranked_portfolios USING (dataset_date)) ranked_forecast_id USING (dataset_date, forecast_id, forecast_type)
         LEFT JOIN ( SELECT topline_master_gl_list.gl_product_group, "max"(topline_master_gl_list.gl_name::text) AS gl_name
                      FROM topline_master_gl_list
                     WHERE topline_master_gl_list.gl_name::text <> 'Unknown'::text
                     GROUP BY topline_master_gl_list.gl_product_group) gl_list USING (gl_product_group)
        WHERE adjusted_orders_forecast.target_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND adjusted_orders_forecast.target_day <= date_add('month'::text, 13::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND adjusted_orders_forecast.gl_product_group < 990 AND adjusted_orders_forecast.gl_rollup::text <> 'dig'::text AND adjusted_orders_forecast.gl_rollup::text <> 'unk'::text AND (adjusted_orders_forecast.country_code::text = 'US'::text OR adjusted_orders_forecast.country_code::text = 'CA'::text OR adjusted_orders_forecast.country_code::text = 'UK'::text OR adjusted_orders_forecast.country_code::text = 'DE'::text OR adjusted_orders_forecast.country_code::text = 'FR'::text OR adjusted_orders_forecast.country_code::text = 'IT'::text OR adjusted_orders_forecast.country_code::text = 'ES'::text OR adjusted_orders_forecast.country_code::text = 'JP'::text OR adjusted_orders_forecast.country_code::text = 'IN'::text) AND (ranked_forecast_id.forecast_rank <= 5 OR adjusted_orders_forecast.dataset_date = '2019-12-29'::date) AND forecast_type::text = 'Adjusted'::text
        GROUP BY ranked_forecast_id.portfolio_id, 2, adjusted_orders_forecast.country_code, adjusted_orders_forecast.channel, adjusted_orders_forecast.gl_rollup, adjusted_orders_forecast.gl_product_group, gl_list.gl_name::character varying, 
              CASE
                  WHEN adjusted_orders_forecast.prime_group::text = 'prm'::text OR adjusted_orders_forecast.prime_group::text = 'stu'::text THEN 'prm'::text
                  ELSE 'npr'::text
              END::character varying, adjusted_orders_forecast.target_day, adjusted_orders_forecast.dataset_date
        UNION 
                 SELECT ranked_forecast_id.portfolio_id, 'System'::text AS initiative, orders_gl_forecasts_daily.country_code, orders_gl_forecasts_daily.channel, orders_gl_forecasts_daily.gl_rollup, orders_gl_forecasts_daily.gl_product_group, gl_list.gl_name::character varying AS gl_name, 
                        CASE
                            WHEN orders_gl_forecasts_daily.prime_group::text = 'prm'::text OR orders_gl_forecasts_daily.prime_group::text = 'stu'::text THEN 'prm'::text
                            ELSE 'npr'::text
                        END::character varying AS prime_group, orders_gl_forecasts_daily.target_day, sum(orders_gl_forecasts_daily.net_ordered_units) AS units_fcst, sum(orders_gl_forecasts_daily.net_ordered_gms) AS rev_fcst, orders_gl_forecasts_daily.dataset_date
                   FROM orders_gl_forecasts_daily
              JOIN ( SELECT order_forecast_ids.forecast_type, ranked_portfolios.portfolio_id, order_forecast_ids.dataset_date, order_forecast_ids.forecast_id, pg_catalog.rank()
                          OVER( 
                          PARTITION BY order_forecast_ids.forecast_type
                          ORDER BY order_forecast_ids.dataset_date DESC) AS forecast_rank
                           FROM ( SELECT DISTINCT 'Baseline'::text AS forecast_type, orders_gl_forecasts_daily.dataset_date, "max"(orders_gl_forecasts_daily.forecast_id) AS forecast_id
                                   FROM orders_gl_forecasts_daily
                                  GROUP BY 1, orders_gl_forecasts_daily.dataset_date
                        UNION 
                                 SELECT DISTINCT 'Adjusted'::text AS forecast_type, adjusted_orders_forecast.dataset_date, "max"(adjusted_orders_forecast.forecast_id) AS forecast_id
                                   FROM adjusted_orders_forecast
                                  GROUP BY 1, adjusted_orders_forecast.dataset_date) order_forecast_ids
                      JOIN ( SELECT prod_portfolios.portfolio_id, prod_portfolios.dataset_date, pg_catalog.rank()
                                  OVER( 
                                  ORDER BY prod_portfolios.dataset_date DESC) AS portfolio_rank
                                   FROM ( SELECT DISTINCT shipment_gl_forecasts_v.portfolio_id, shipment_gl_forecasts_v.dataset_date
                                           FROM shipment_gl_forecasts_v
                                          WHERE shipment_gl_forecasts_v.portfolio_status::text = 'PRODUCTION'::text AND shipment_gl_forecasts_v.is_latest_publish::text = 'Y'::text) prod_portfolios) ranked_portfolios USING (dataset_date)) ranked_forecast_id USING (dataset_date, forecast_id, forecast_type)
         LEFT JOIN ( SELECT topline_master_gl_list.gl_product_group, "max"(topline_master_gl_list.gl_name::text) AS gl_name
                      FROM topline_master_gl_list
                     WHERE topline_master_gl_list.gl_name::text <> 'Unknown'::text
                     GROUP BY topline_master_gl_list.gl_product_group) gl_list USING (gl_product_group)
        WHERE orders_gl_forecasts_daily.target_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND orders_gl_forecasts_daily.target_day <= date_add('month'::text, 13::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND orders_gl_forecasts_daily.gl_product_group < 990 AND orders_gl_forecasts_daily.gl_rollup <> 'dig'::bpchar AND orders_gl_forecasts_daily.gl_rollup <> 'unk'::bpchar AND (orders_gl_forecasts_daily.country_code = 'US'::bpchar OR orders_gl_forecasts_daily.country_code = 'CA'::bpchar OR orders_gl_forecasts_daily.country_code = 'UK'::bpchar OR orders_gl_forecasts_daily.country_code = 'DE'::bpchar OR orders_gl_forecasts_daily.country_code = 'FR'::bpchar OR orders_gl_forecasts_daily.country_code = 'IT'::bpchar OR orders_gl_forecasts_daily.country_code = 'ES'::bpchar OR orders_gl_forecasts_daily.country_code = 'JP'::bpchar OR orders_gl_forecasts_daily.country_code = 'IN'::bpchar) AND (ranked_forecast_id.forecast_rank <= 5 OR orders_gl_forecasts_daily.dataset_date = '2019-12-29'::date) AND forecast_type = 'Baseline'::bpchar
        GROUP BY ranked_forecast_id.portfolio_id, 2, orders_gl_forecasts_daily.country_code, orders_gl_forecasts_daily.channel, orders_gl_forecasts_daily.gl_rollup, orders_gl_forecasts_daily.gl_product_group, gl_list.gl_name::character varying, 
              CASE
                  WHEN orders_gl_forecasts_daily.prime_group::text = 'prm'::text OR orders_gl_forecasts_daily.prime_group::text = 'stu'::text THEN 'prm'::text
                  ELSE 'npr'::text
              END::character varying, orders_gl_forecasts_daily.target_day, orders_gl_forecasts_daily.dataset_date) "of"
      FULL JOIN ( SELECT order_actuals_osa_v_q4_test.country_code, order_actuals_osa_v_q4_test.channel, order_actuals_osa_v_q4_test.gl_rollup, order_actuals_osa_v_q4_test.gl_product_group, gl_list.gl_name, 
                        CASE
                            WHEN order_actuals_osa_v_q4_test.prime_group::text = 'prm'::text OR order_actuals_osa_v_q4_test.prime_group::text = 'stu'::text THEN 'prm'::text
                            ELSE 'npr'::text
                        END AS prime_group, order_actuals_osa_v_q4_test.order_day AS target_day, sum(order_actuals_osa_v_q4_test.net_ordered_units) AS units_act, sum(order_actuals_osa_v_q4_test.net_ordered_gms) AS rev_act
                   FROM order_actuals_osa_v_q4_test
              LEFT JOIN ( SELECT topline_master_gl_list.gl_product_group, "max"(topline_master_gl_list.gl_name::text) AS gl_name
                           FROM topline_master_gl_list
                          WHERE topline_master_gl_list.gl_name::text <> 'Unknown'::text
                          GROUP BY topline_master_gl_list.gl_product_group) gl_list USING (gl_product_group)
             WHERE order_actuals_osa_v_q4_test.order_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND order_actuals_osa_v_q4_test.gl_product_group < 990 AND order_actuals_osa_v_q4_test.gl_rollup <> 'dig'::bpchar AND order_actuals_osa_v_q4_test.gl_rollup <> 'unk'::bpchar AND (order_actuals_osa_v_q4_test.country_code = 'US'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'CA'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'UK'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'DE'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'FR'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'IT'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'ES'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'JP'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'IN'::bpchar)
             GROUP BY order_actuals_osa_v_q4_test.country_code, order_actuals_osa_v_q4_test.channel, order_actuals_osa_v_q4_test.gl_rollup, order_actuals_osa_v_q4_test.gl_product_group, gl_list.gl_name, 
                   CASE
                       WHEN order_actuals_osa_v_q4_test.prime_group::text = 'prm'::text OR order_actuals_osa_v_q4_test.prime_group::text = 'stu'::text THEN 'prm'::text
                       ELSE 'npr'::text
                   END, order_actuals_osa_v_q4_test.order_day) oa USING (country_code, channel, gl_rollup, gl_product_group, gl_name, prime_group, target_day)) d
   LEFT JOIN ( SELECT topline_fx_rates.source_country_code AS country_code, topline_fx_rates.fx_rate, topline_fx_rates.updated_date
           FROM topline_fx_rates
          WHERE topline_fx_rates.target_country_code = 'US'::bpchar AND topline_fx_rates.scenario::text = '2019 OP2'::text AND (topline_fx_rates.source_country_code = 'US'::bpchar OR topline_fx_rates.source_country_code = 'CA'::bpchar OR topline_fx_rates.source_country_code = 'UK'::bpchar OR topline_fx_rates.source_country_code = 'DE'::bpchar OR topline_fx_rates.source_country_code = 'FR'::bpchar OR topline_fx_rates.source_country_code = 'IT'::bpchar OR topline_fx_rates.source_country_code = 'ES'::bpchar OR topline_fx_rates.source_country_code = 'JP'::bpchar OR topline_fx_rates.source_country_code = 'IN'::bpchar)) fx ON d.country_code = fx.country_code
   LEFT JOIN ( SELECT 'Shipments'::text AS ship_order, ship_act.country_code, "max"(ship_act.target_day) AS latest_act
      FROM ( SELECT shipment_gl_actuals_v.country_code, shipment_gl_actuals_v.channel, shipment_gl_actuals_v.gl_rollup, shipment_gl_actuals_v.gl_product_group, shipment_gl_actuals_v.gl_name, 
                   CASE
                       WHEN shipment_gl_actuals_v.prime_group::text = 'prm'::text OR shipment_gl_actuals_v.prime_group::text = 'stu'::text THEN 'prm'::text
                       ELSE 'npr'::text
                   END AS prime_group, shipment_gl_actuals_v.target_day, sum(shipment_gl_actuals_v.shipped_units) AS units_act, sum(
                   CASE
                       WHEN shipment_gl_actuals_v.channel::text = 'ret'::text THEN shipment_gl_actuals_v.product_revenue_amt
                       ELSE shipment_gl_actuals_v.gross_gms_amt
                   END) AS rev_act
              FROM shipment_gl_actuals_v
             WHERE shipment_gl_actuals_v.target_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND shipment_gl_actuals_v.gl_product_group < 990 AND shipment_gl_actuals_v.gl_rollup <> 'dig'::bpchar AND shipment_gl_actuals_v.gl_rollup <> 'unk'::bpchar AND (shipment_gl_actuals_v.country_code = 'US'::bpchar OR shipment_gl_actuals_v.country_code = 'CA'::bpchar OR shipment_gl_actuals_v.country_code = 'UK'::bpchar OR shipment_gl_actuals_v.country_code = 'DE'::bpchar OR shipment_gl_actuals_v.country_code = 'FR'::bpchar OR shipment_gl_actuals_v.country_code = 'IT'::bpchar OR shipment_gl_actuals_v.country_code = 'ES'::bpchar OR shipment_gl_actuals_v.country_code = 'JP'::bpchar OR shipment_gl_actuals_v.country_code = 'IN'::bpchar)
             GROUP BY shipment_gl_actuals_v.country_code, shipment_gl_actuals_v.channel, shipment_gl_actuals_v.gl_rollup, shipment_gl_actuals_v.gl_product_group, shipment_gl_actuals_v.gl_name, 
                   CASE
                       WHEN shipment_gl_actuals_v.prime_group::text = 'prm'::text OR shipment_gl_actuals_v.prime_group::text = 'stu'::text THEN 'prm'::text
                       ELSE 'npr'::text
                   END, shipment_gl_actuals_v.target_day) ship_act
     GROUP BY 1, ship_act.country_code
UNION 
    SELECT 'Orders'::text AS ship_order, ord_act.country_code, "max"(ord_act.target_day) AS latest_act
      FROM ( SELECT order_actuals_osa_v_q4_test.country_code, order_actuals_osa_v_q4_test.channel, order_actuals_osa_v_q4_test.gl_rollup, order_actuals_osa_v_q4_test.gl_product_group, gl_list.gl_name, 
                   CASE
                       WHEN order_actuals_osa_v_q4_test.prime_group::text = 'prm'::text OR order_actuals_osa_v_q4_test.prime_group::text = 'stu'::text THEN 'prm'::text
                       ELSE 'npr'::text
                   END AS prime_group, order_actuals_osa_v_q4_test.order_day AS target_day, sum(order_actuals_osa_v_q4_test.net_ordered_units) AS units_act, sum(order_actuals_osa_v_q4_test.net_ordered_gms) AS rev_act
              FROM order_actuals_osa_v_q4_test
         LEFT JOIN ( SELECT topline_master_gl_list.gl_product_group, "max"(topline_master_gl_list.gl_name::text) AS gl_name
                      FROM topline_master_gl_list
                     WHERE topline_master_gl_list.gl_name::text <> 'Unknown'::text
                     GROUP BY topline_master_gl_list.gl_product_group) gl_list USING (gl_product_group)
        WHERE order_actuals_osa_v_q4_test.order_day >= date_add('month'::text, -25::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND order_actuals_osa_v_q4_test.gl_product_group < 990 AND order_actuals_osa_v_q4_test.gl_rollup <> 'dig'::bpchar AND order_actuals_osa_v_q4_test.gl_rollup <> 'unk'::bpchar AND (order_actuals_osa_v_q4_test.country_code = 'US'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'CA'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'UK'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'DE'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'FR'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'IT'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'ES'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'JP'::bpchar OR order_actuals_osa_v_q4_test.country_code = 'IN'::bpchar)
        GROUP BY order_actuals_osa_v_q4_test.country_code, order_actuals_osa_v_q4_test.channel, order_actuals_osa_v_q4_test.gl_rollup, order_actuals_osa_v_q4_test.gl_product_group, gl_list.gl_name, 
              CASE
                  WHEN order_actuals_osa_v_q4_test.prime_group::text = 'prm'::text OR order_actuals_osa_v_q4_test.prime_group::text = 'stu'::text THEN 'prm'::text
                  ELSE 'npr'::text
              END, order_actuals_osa_v_q4_test.order_day) ord_act
     GROUP BY 1, ord_act.country_code) l ON d.ship_order::text = l.ship_order AND d.country_code = l.country_code;


GRANT SELECT ON toplinef_test.tableau_wbr_dashboard_v TO toplinef_temp;
GRANT SELECT, INSERT, TRIGGER, RULE, DELETE, REFERENCES, UPDATE ON toplinef_test.tableau_wbr_dashboard_v TO toplinef_ro;
GRANT SELECT ON toplinef_test.tableau_wbr_dashboard_v TO topline_tableau;


COMMIT;
