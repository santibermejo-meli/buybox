create multiset volatile table DOMAINS as (
select distinct prd.dom_domain_id
from whowner.LK_BUYBOX_PRODUCT_STATUS prd_stat
left join WHOWNER.LK_PRD_DOMAIN_PRODUCTS prd 
  on prd.prd_product_id = prd_stat.prd_product_id 
    and prd.sit_site_id = prd_stat.sit_site_id
where status = 'active'
)
with data primary index (dom_domain_id) on commit preserve rows;

DELETE FROM TABLEAU_TBL.DM_BUYBOX_ORDERS WHERE TIM_DAY_WINNING_DATE = DATE - 1;

INSERT INTO TABLEAU_TBL.DM_BUYBOX_ORDERS
SELECT B.TIM_DAY_WINNING_DATE AS TIM_DAY_WINNING_DATE,
      B.SIT_SITE_ID AS SIT_SITE_ID,
      B.DOM_DOMAIN_ID AS DOM_DOMAIN_ID,
      B.BID_SELL_REP_LEVEL AS BID_SELL_REP_LEVEL,
      s.seller_segmento as SEGMENTO,
      MT.MAPP_MOBILE_FLAG,
      COALESCE(SUM(b.BID_QUANTITY_OK),0) SI_TS,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 OR 
                    B.ITE_VAR_OPT_ELEGIBLE = 1 OR
                    B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK END),0) SI_OPTINEABLE,
      COALESCE(SUM(CASE WHEN B.ITE_VAR_OPT_ELEGIBLE = 1 THEN b.BID_QUANTITY_OK END),0) SI_OPTINEABLE_FUERA_BB,
      COALESCE(SUM(CASE WHEN B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK END),0) SI_CLON,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 THEN b.BID_QUANTITY_OK END),0) SI_BB,
      COALESCE(SUM(b.BID_QUANTITY_OK * B.bid_current_price),.0) GMV_TS,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 OR 
                    B.ITE_VAR_OPT_ELEGIBLE = 1 OR
                    B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK * b.bid_current_price END),.0) GMV_OPTINEABLE,
      COALESCE(SUM(CASE WHEN B.ITE_VAR_OPT_ELEGIBLE = 1 THEN b.BID_QUANTITY_OK * b.bid_current_price END),.0) GMV_OPTINEABLE_FUERA_BB,
      COALESCE(SUM(CASE WHEN B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK * b.bid_current_price END),.0) GMV_CLON,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 THEN b.BID_QUANTITY_OK * b.bid_current_price END),.0) GMV_BB
FROM BT_BIDS B
JOIN DOMAINS d
  ON d.dom_domain_id = b.DOM_DOMAIN_ID
LEFT JOIN LKV_MKPL_SEGMENTACION_SELLER s
  on b.cus_cust_id_sel = s.CUS_CUST_ID_SEL
left join WHOWNER.LK_MAPP_MOBILE mp 
  on (coalesce(b.MAPP_APP_ID, '-1') = mp.MAPP_APP_ID) 
left join WHOWNER.LK_MAPP_MOBILE_TYPES mt
  on (case when mp.MAPP_APP_DESC like 'VIP-Mobile%' then 'Mobile-WEB' 
    else mp.MAPP_APP_DESC end = mt.MAPP_APP_TYPE)
WHERE B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE = date -1
GROUP BY 1,2,3,4,5,6;

INSERT INTO TABLEAU_TBL.DM_BUYBOX_ORDERS
SELECT B.TIM_DAY_WINNING_DATE AS TIM_DAY_WINNING_DATE,
      B.SIT_SITE_ID AS SIT_SITE_ID,
      'TOTAL SITE' as DOM_DOMAIN_ID,
      null as BID_SELL_REP_LEVEL,
      null AS SEGMENTO,
      null AS MAPP_MOBILE_FLAG,
      COALESCE(SUM(b.BID_QUANTITY_OK),0) SI_TS,
      0 as SI_OPTINEABLE,
      0 as SI_OPTINEABLE_FUERA_BB,
      0 as SI_CLON,
      0 as SI_BB,
      COALESCE(SUM(b.BID_QUANTITY_OK * B.bid_current_price),0) GMV_TS,
      .0 as GMV_OPTINEABLE,
      .0 as GMV_OPTINEABLE_FUERA_BB,
      .0 as GMV_CLON,
      .0 as GMV_BB
FROM BT_BIDS B
WHERE B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE = date -1
  AND B.sit_site_id IN ('MLA','MLB','MLM')
GROUP BY 1,2,3,4,5,6;
