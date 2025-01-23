with final_view as (
SELECT sf_account.name AS accountName,
       advertiser_category.advertiser_category_name AS advertiserCategory,
       agency.name AS agencyName,
       campaigns.cid AS alphCid,
       ROUND(SUM(IFNULL(billing_daily_impressions.billable_impr, 0) * (os.percentage / 100)), 2) AS billableImpr,
       CASE
           WHEN sf_opportunity.billing_level = 'Agency' THEN agency.name
           ELSE sf_account.name
       END AS billingAccount,
       sf_opportunity.brand_name AS brandName,
       os.business_vertical AS businessVertical,
       campaigns.campaign_name AS campaignName,
       campaigns.campaign_type AS campaignType,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN ct.continental_region
           ELSE countries.continental_region
       END AS continentalRegion,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN ct.country_code_2
           ELSE countries.country_code_2
       END AS countryCode2,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN ct.country_code_3
           ELSE placement.country_code
       END AS countryCode3,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.country
           ELSE countries.country_name
       END AS countryName,
       sf_opportunity.currency_iso_code AS currencyIsoCode,
       -- DATE_FORMAT(date, \"%Y-%m-%d\") AS date,
       TO_DATE(date, 'YYYY-MM-DD') AS date,
       SUM((IFNULL(impressions, 0) + IFNULL(delivered_adjusted_impr, 0)) * (os.percentage / 100)) AS deliveredImpressions,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.device_type
           ELSE placement.device_types
       END AS deviceTypes,
       SUM(IFNULL(billing_daily_impressions.final_revenue, 0) * (os.percentage / 100)) AS finalRevenue,
       SUM(IFNULL(billing_daily_impressions.final_revenue_usd, 0) * (os.percentage / 100)) AS finalRevenueUsd,
        CASE
           WHEN sf_opportunity.billing_level in ('Agency') THEN agency_parent.name
           ELSE account_parent.name
       END AS holdingCompany, 
       SUM((IFNULL(impressions, 0)) * (os.percentage / 100)) AS impressions,
       sf_opportunity.industry_type AS industryType,
       gp.io_op_placement_id AS ioOpPlacementId,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.product_full_name
           ELSE placement.billing_name
       END AS ioOpPlacementName,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.placement_type
           ELSE placement.type
       END AS ioOpPlacementType,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN ct.lge_region
           ELSE countries.lge_region
       END AS lgeRegion,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.media_type_formula
           ELSE placement.media_type
       END AS mediaType,
       MONTHNAME(date) AS MONTH,
       gp.op_placement_ids AS opPlacementIds,
       sf_opportunity.id AS opportunityId,
       sf_opportunity.name AS opportunityName,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.placement_category
           ELSE placement.category
       END AS placementCategory,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.end_time_io_timezone
           ELSE placement.end_date
       END AS placementEndDate,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.start_time_io_timezone
           ELSE placement.start_date
       END AS placementStartDate,
       pods.pod_name AS podName,
       sf_pricebook.name AS pricebookName,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN gp.gp_product_type
           ELSE CASE
                    WHEN media_type = 'Display' THEN CASE
                                                         WHEN device_types LIKE '%CTV%' THEN 'CTV Native'
                                                         ELSE 'Display'
                                                     END
                    ELSE CASE
                             WHEN device_types LIKE '%CTV%' THEN 'CTV Video'
                             ELSE 'Video'
                         END
                END
       END AS productType,
       QUARTER(date) AS QUARTER,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.rate
           ELSE placement.rate
       END AS rate,
       CASE
           WHEN gp.has_io_placement_id = 1 THEN sf_opportunity_product.buy_type
           ELSE placement.rate_type
       END AS rateType,
       'Managed Services' AS SEGMENT,
       sellers.name AS sellerName,
       os.percentage AS sellerPercentage,
       campaigns.reseller AS reseller,
       campaigns.cs_id AS csId,
       campaigns.bda_id AS bdaId,
       campaigns.am_id AS amId
FROM billing_staging.campaigns_db_v2_billing_staging.billing_daily_impressions billing_daily_impressions
INNER JOIN billing_staging.campaigns_db_v2_billing_staging.billing_placement_status billing_placement_status
 ON (billing_daily_impressions.placement_status_id = billing_placement_status.placement_status_id)
LEFT JOIN
  (SELECT id,
          io_placement_id
   FROM billing_staging.campaigns_db_v2_billing_staging.placement placement) AS pl 
        ON (billing_placement_status.io_op_placement_id = pl.id)
LEFT JOIN
  (SELECT billing_adjustments.adjustment_id AS adjustment_id,
          billing_adjustments_meta.placement_status_id,
          SUM(CASE
                  WHEN billed_impr_change = 1::boolean THEN adjusted_impr
                  ELSE 0
              END) AS billing_adjusted_impr,
          SUM(CASE
                  WHEN delivered_impr_change = 1::boolean THEN adjusted_impr
                  ELSE 0
              END) AS delivered_adjusted_impr,
          billing_adjustments.revenue_date AS adjustment_revenue_date,
          billing_adjustments.adjustment_date
   FROM billing_staging.campaigns_db_v2_billing_staging.billing_adjustments_meta billing_adjustments_meta
   INNER JOIN billing_staging.campaigns_db_v2_billing_staging.billing_adjustments billing_adjustments 
      ON billing_adjustments.adjustment_id = billing_adjustments_meta.adjustment_id
   WHERE delivered_impr_change = 1::boolean
     AND billing_adjustments.revenue_date >= '2025-01-01'::date
     AND billing_adjustments.revenue_date <= '2025-01-17'::date
   GROUP BY billing_adjustments.adjustment_id, billing_adjustments_meta.placement_status_id,
            billing_adjustments.revenue_date,billing_adjustments.adjustment_date) AS adjustments ON adjustments.placement_status_id = billing_daily_impressions.placement_status_id
AND adjustments.adjustment_revenue_date = billing_daily_impressions.date
INNER JOIN
  (SELECT max(cid) AS cid,
          min(converted_start_date) AS start_date,
          max(converted_end_date) AS end_date,
          sum(spend) AS spend,
          max(CASE
                  WHEN media_type = 'Display' THEN CASE
                                                       WHEN device_types LIKE '%CTV%' THEN 'CTV Native'
                                                       ELSE 'Display'
                                                   END
                  ELSE CASE
                           WHEN device_types LIKE '%CTV%' THEN 'CTV Video'
                           ELSE 'Video'
                       END
              END) AS gp_product_type,
          sum(impression_budget) AS impression_budget,
          max(placement.name) AS name,
          max(placement.dsp) AS dsp,
          max(CASE
                  WHEN io_placement_id IS NOT NULL THEN 1
                  ELSE 0
              END) AS has_io_placement_id,
          (CASE
               WHEN io_placement_id IS NOT NULL THEN io_placement_id
               ELSE id
           END) AS io_op_placement_id,
          --GROUP_CONCAT(id
            --           ORDER BY id ASC SEPARATOR  ', ' ) AS op_placement_ids
         concat_ws(sort_array(collect_list(id))::string,', ' ) AS op_placement_ids
   FROM billing_staging.campaigns_db_v2_billing_staging.placement placement
   GROUP BY    io_op_placement_id) AS gp 
ON (CASE
                                              WHEN pl.io_placement_id IS NULL THEN billing_placement_status.io_op_placement_id
                                              ELSE pl.io_placement_id
                                          END = gp.io_op_placement_id)

LEFT JOIN (SELECT 
            id,io_placement_id, country_code, device_types, billing_name, type, media_type,category, end_date, start_date, rate_type, rate
          FROM billing_staging.campaigns_db_v2_billing_staging.placement) placement ON (placement.id = gp.io_op_placement_id)
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_opportunity_product sf_opportunity_product ON (gp.io_op_placement_id = sf_opportunity_product.id)
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_product ON (sf_product.id = sf_opportunity_product.product_id)
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.countries AS ct ON sf_opportunity_product.country = ct.country_name
INNER JOIN billing_staging.campaigns_db_v2_billing_staging.campaigns ON campaigns.cid = gp.cid
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.pods ON campaigns.pod_id = pods.pod_id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.advertiser ON campaigns.advertiser_id = advertiser.alph_brand_id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.advertiser_category ON advertiser.advertiser_category_id = advertiser_category.advertiser_category_id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_opportunity ON sf_opportunity.id = campaigns.opportunity_id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_account sf_account ON sf_opportunity.account_id = sf_account.id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_account AS account_parent ON sf_account.parent_id = account_parent.id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_pricebook ON sf_opportunity.pricebook_id = sf_pricebook.id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_account AS agency ON sf_opportunity.agency_id = agency.id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_account AS agency_parent ON agency.parent_id = agency_parent.id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_opportunity_seller AS os ON sf_opportunity.id = os.opportunity_id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.sf_user AS sellers ON os.seller_id = sellers.id
LEFT JOIN billing_staging.campaigns_db_v2_billing_staging.countries AS countries ON countries.country_code_3 = placement.country_code
WHERE 
  billing_daily_impressions.date >= '2025-01-01'::DATE
  AND billing_daily_impressions.date <= '2025-01-17'::DATE
GROUP BY accountName,
         advertiserCategory,
         agencyName,
         alphCid,
         billingAccount,
         brandName,
         businessVertical,
         campaignName,
         campaignType,
         continentalRegion,
         countryCode2,
         countryCode3,
         countryName,
         currencyIsoCode, date, deviceTypes,
                                holdingCompany,
                                industryType,
                                ioOpPlacementId,
                                ioOpPlacementName,
                                ioOpPlacementType,
                                lgeRegion,
                                mediaType,
                                MONTH,
                                opPlacementIds,
                                opportunityId,
                                opportunityName,
                                placementCategory,
                                placementEndDate,
                                placementStartDate,
                                podName,
                                pricebookName,
                                QUARTER,
                                sf_opportunity_product.rate,
                                rateType,
                                SEGMENT,
                                sellerName,
                                sellerPercentage,
                                subSegment,
                                YEAR,
                                productType,
                                reseller,
                                csId,
                                bdaId,
                                amId,
                                pl.io_placement_id,
                                billing_placement_status.has_io_placement_id,
                                placement.rate)
select *
from final_view
