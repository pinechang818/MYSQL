with category_Mapping as
    (
        select distinct
            category_desc
          , semantic_cluster
        from
            TDC_PROD.demand_category_mapping
    )
  , gl_0 as
    (
        select *
        from
            toplinef_test.accheria_COVID_gl_promise_extension
        where
            gl_product_group = 0
    )
  , data as
    (
        select
            region
          , country    as country_code
          , fn_channel as channel
          , a.gl_product_group
          , wbr_gl_description
          , wbr_gl_family
          , in_country
          , in_region
          , case
                when semantic_cluster in ('Children'
                                        ,'WFH Essentials')
                    then 'WFH/Kids Essentials'
                when semantic_cluster in ('Household Care'
                                        ,'Hygiene/Medical Supplies')
                    then 'Hygiene/Medical/ Household Essentials'
                when semantic_cluster in ('Grocery')
                    then 'Grocery'
                when wbr_gl_family = 'Softlines'
                    then 'Softlines'
                    else 'Others'
            end as GL_Custom_group
          , case
                when cv.promise_extension is not null
                    then cv.promise_extension
                    else coalesce(gl_0.promise_extension,'Extension_None')
            end                      as Promise_extension
          , coalesce(glB.boss, 'No') as GL_Boss_group
          , gvs
          , '' category_desc
          , snapshot_day
          , sum(gv_count) as gv_count
          , sum(gv_count*LEAST(gvs_prod_int,35)) as gvs_prod_sum
        from
            toplinef_ddl.speed_agg a
            join
                marketplaces
                on
                    marketplace_id = id
            join
                toplinef_test.accheria_tableau_promise_dashboard_reference
                on
                    coalesce(gvs_prod_int,30) between range_min and range_max
            left join
                category_Mapping
        using (category_desc)
            left join
                toplinef_test.accheria_COVID_gl_promise_extension cv
                on
                    country                = cv.country_code
                    and a.gl_product_group = cv.gl_product_group
                    and a.category_code    = coalesce(cv.category_code,a.category_code)
            left join
                gl_0
                on
                    country = gl_0.country_code
            left join
                toplinef_test.accheria_COVID_FR_IT_BOSSing glB
                on
                    country                = glB.country_code
                    and a.gl_product_group = glB.gl_product_group
                    and a.category_code    = glB.category_code
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    )
select *
from
    data