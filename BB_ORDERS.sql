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
SELECT B.TIM_DAY_WINNING_DATE,
      B.SIT_SITE_ID,
      B.DOM_DOMAIN_ID,
      B.BID_SELL_REP_LEVEL,
      (CASE WHEN COALESCE(B.ITE_OFFICIAL_STORE_ID, 0) > 0 THEN 'TIENDA OFICIAL'
             WHEN COALESCE(B.ITE_OFFICIAL_STORE_ID, 0) = 0 AND G.CUS_CUST_ID_SEL IS NOT NULL THEN 'CARTERA GESTIONADA'
       ELSE 'LONG TAIL' END) AS SEGMENTO,
      COALESCE(SUM(b.BID_QUANTITY_OK),0) SI_TS,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 OR 
                    B.ITE_VAR_OPT_ELEGIBLE = 1 OR
                    B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK END),0) SI_OPTINEABLE,
      COALESCE(SUM(CASE WHEN B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK END),0) SI_FUERA_BB,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 THEN b.BID_QUANTITY_OK END),0) SI_BB,
      COALESCE(SUM(b.BID_QUANTITY_OK * B.BID_SITE_CURRENT_PRICE),0) GMV_TS,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 OR 
                    B.ITE_VAR_OPT_ELEGIBLE = 1 OR
                    B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK * b.BID_SITE_CURRENT_PRICE END),0) GMV_OPTINEABLE,
      COALESCE(SUM(CASE WHEN B.ITE_VAR_OPT_ALREADY_OPTED_IN = 1 THEN b.BID_QUANTITY_OK * b.BID_SITE_CURRENT_PRICE END),0) GMV_FUERA_BB,
      COALESCE(SUM(CASE WHEN B.ITE_CATALOG_LISTING = 1 THEN b.BID_QUANTITY_OK * b.BID_SITE_CURRENT_PRICE END),0) GMV_BB,
      CASE WHEN SI_TS = 0 THEN 0 ELSE GMV_TS/SI_TS END AS ASP_TS,
      CASE WHEN SI_OPTINEABLE = 0 THEN 0 ELSE GMV_OPTINEABLE/SI_OPTINEABLE END AS ASP_OPTINEABLE,
      CASE WHEN SI_FUERA_BB = 0 THEN 0 ELSE GMV_FUERA_BB/SI_FUERA_BB END AS ASP_FUERA_BB,
      CASE WHEN SI_BB = 0 THEN 0 ELSE GMV_BB/SI_BB END AS ASP_BB
FROM BT_BIDS B
JOIN DOMAINS d
  ON d.dom_domain_id = b.DOM_DOMAIN_ID
LEFT JOIN WHOWNER.LK_SALES_CARTERA_GESTIONADA AS G
  ON B.CUS_CUST_ID_SEL = G.CUS_CUST_ID_SEL
    AND COALESCE(B.ITE_OFFICIAL_STORE_ID, 0) = G.ITE_OFFICIAL_STORE_ID
    AND B.TIM_DAY_WINNING_DATE BETWEEN G.FECHA_DESDE AND COALESCE(G.FECHA_HASTA, DATE + 1)
WHERE B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE = date - 1
GROUP BY 1,2,3,4,5;
